/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2017 Cinchapi Inc.
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.cinchapi.concourse.example.bank;

import java.util.ArrayDeque;
import java.util.Set;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.Link;
import com.cinchapi.concourse.TransactionException;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * An implementation of the {@link Account} interface that uses Concourse for
 * data storage.
 * 
 * @author Jeff Nelson
 */
public class ConcourseAccount implements Account {

    // NOTE: For the purpose of this example, we don't store any data about the
    // Account locally so that we can illustrate how you would query Concourse
    // to get the data. In a real application, you always want to cache
    // repeatedly used data locally.

    /**
     * Since Concourse does not have tables, we store a special key in each
     * record to indicate the class to which the record/object belongs. This
     * isn't necessary, but it helps to ensure logical consistency between the
     * application and the database.
     */
    private final static String CLASS_KEY_NAME = "_class";

    /**
     * The id of the Concourse record that holds the data for an instance of
     * this class.
     */
    private final long id;

    /**
     * This constructor creates a new record in Concourse and inserts the data
     * expressed in the parameters.
     * 
     * @param balance the initial balance for the account
     * @param owners {@link Customer customers} that are owners on the account
     */
    public ConcourseAccount(double balance, Customer... owners) {
        Preconditions.checkArgument(balance > 0);
        Concourse concourse = Constants.CONCOURSE_CONNECTIONS.request();
        try {
            Multimap<String, Object> data = HashMultimap.create();
            data.put(CLASS_KEY_NAME, getClass().getName());
            data.put("balance", balance);
            for (Customer owner : owners) {
                data.put("owners", Link.to(owner.getId()));
            }
            this.id = concourse.insert(data); // The #insert method is atomic,
                                              // so we don't need a transaction
                                              // to safely commit all the data
        }
        finally {
            Constants.CONCOURSE_CONNECTIONS.release(concourse);
        }
    }

    /**
     * This constructor loads an existing object from Concourse.
     * 
     * @param id the id of the record that holds the data for the object we want
     *            to load
     */
    public ConcourseAccount(long id) {
        Concourse concourse = Constants.CONCOURSE_CONNECTIONS.request();
        try {
            Preconditions.checkArgument(getClass().getName().equals(
                    concourse.get(CLASS_KEY_NAME, id)));
            this.id = id;
            // NOTE: If this were a real application, it would be a god idea to
            // preload frequently used attributes here and cache them locally
            // (or maybe even in an external cache like Memcache or Redis).
        }
        finally {
            Constants.CONCOURSE_CONNECTIONS.release(concourse);
        }
    }

    @Override
    public boolean debit(String charge, double amount) {
        Preconditions.checkArgument(amount > 0);
        Concourse concourse = Constants.CONCOURSE_CONNECTIONS.request();
        try {
            concourse.stage();
            if(withdrawImpl(concourse, amount)) {
                // By using the #add method, we can store multiple charges in
                // the record at the same time
                concourse.add("charges", charge, id);
                return concourse.commit();
            }
            else {
                concourse.abort();
                return false;
            }
        }
        catch (TransactionException e) {
            concourse.abort();
            return false;
        }
        finally {
            Constants.CONCOURSE_CONNECTIONS.release(concourse);
        }
    }

    @Override
    public boolean deposit(double amount) {
        Preconditions.checkArgument(amount > 0);
        // This implementation uses verifyAndSwap to atomically increment the
        // account balance (without a using a transaction!) which ensures that
        // money isn't lost if two people make a deposit at the same time.
        Concourse concourse = Constants.CONCOURSE_CONNECTIONS.request();
        try {
            boolean incremented = false;
            while (!incremented) {
                double balance = concourse.get("balance", id);
                double newBalance = balance + amount;
                if(concourse.verifyAndSwap("balance", balance, id, newBalance)) {
                    incremented = true;
                }
                else {
                    // verifyAndSwap will fail if the balance changed since the
                    // initial read. If that happens, we simply retry
                    continue;
                }
            }
            return true;
        }
        finally {
            Constants.CONCOURSE_CONNECTIONS.release(concourse);
        }
    }

    @Override
    public double getBalance() {
        Concourse concourse = Constants.CONCOURSE_CONNECTIONS.request();
        try {
            return concourse.get("balance", id);
        }
        finally {
            Constants.CONCOURSE_CONNECTIONS.release(concourse);
        }
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public Customer[] getOwners() {
        Concourse concourse = Constants.CONCOURSE_CONNECTIONS.request();
        try {
            Set<Link> customerLinks = concourse.select("owners", id);
            Customer[] owners = new Customer[customerLinks.size()];
            int index = 0;
            for (Link link : customerLinks) {
                owners[index] = new ConcourseCustomer(link.longValue());
                ++index;
            }
            return owners;
        }
        finally {
            Constants.CONCOURSE_CONNECTIONS.release(concourse);
        }
    }

    @Override
    public boolean withdraw(double amount) {
        return debit("withdraw " + amount, amount);
    }

    /**
     * An internal method that transfers money (if possible) from this account
     * to an {@code other} one. This method doesn't start a transaction because
     * it assumes that the caller has already done so.
     * 
     * @param concourse the connection to Concourse that is retrieved from
     *            {@link Constants#CONCOURSE_CONNECTIONS}
     * @param other the recipient {@link ConcourseAccount account} for the
     *            transferred funds
     * @param amount the amount to transfer from this account.
     * @return the amount of money that is actually transferred from this
     *         account to {@code other}. An account can only transfer as much
     *         money as it has. So, if this account has a balance that is
     *         smaller than {@code amount}, it will transfer only how much it
     *         has.
     */
    private double transferTo(Concourse concourse, ConcourseAccount other,
            double amount) {
        double balance = concourse.get("balance", id);
        if(balance > 0) {
            double toTransfer = balance > amount ? amount : balance;
            balance -= toTransfer;
            double otherBalance = concourse.get("balance", other.getId());
            otherBalance += toTransfer;
            concourse.set("balance", balance, id);
            concourse.set("balance", otherBalance, other.getId());
            concourse.add("charges", "transfer " + toTransfer + " to account "
                    + other.getId(), id);
            return toTransfer;
        }
        else {
            return 0;
        }
    }

    /**
     * An implementation that withdraws {@code amount} of money from this
     * account using the provided {@code concourse} connection. This method does
     * not start a new transaction because it assumes that the caller has
     * already done so.
     * 
     * @param concourse the connection to Concourse that is retrieved from
     *            {@link Constants#CONCOURSE_CONNECTIONS}
     * @param amount the amount to withdraw
     * @return {@code true} if the withdrawal is successful (e.g. there is
     *         enough money in the account (possibly after transferring from
     *         other accounts) to withdraw the money without leaving a negative
     *         balance).
     */
    private boolean withdrawImpl(Concourse concourse, double amount) {
        double balance = concourse.get("balance", id);
        double need = amount - balance;
        if(need > 0) {
            // Get all the other accounts that are owned by the owners of this
            // account
            StringBuilder criteria = new StringBuilder();
            criteria.append(CLASS_KEY_NAME).append(" = ")
                    .append(getClass().getName());
            criteria.append(" AND (");
            boolean first = true;
            for (Customer owner : getOwners()) {
                if(!first) {
                    criteria.append(" OR ");
                }
                first = false;
                criteria.append("owners lnks2 ").append(owner.getId());
            }
            criteria.append(")");
            Set<Long> otherAccounts = concourse.find(criteria.toString());
            ArrayDeque<Long> stack = new ArrayDeque<Long>(otherAccounts);
            while (need > 0 && !stack.isEmpty()) {
                long rid = stack.pop();
                if(rid == id) {
                    // Don't try to transfer money from the same account as
                    // this!
                    continue;
                }
                ConcourseAccount other = new ConcourseAccount(rid);
                double transferred = other.transferTo(concourse, this, need);
                // NOTE: We don't need to worry about the balance changing by
                // another client. If that happens, the transaction will
                // automatically fail
                balance += transferred;
                need -= transferred;
            }
            if(need > 0) {
                return false;
            }
        }
        concourse.set("balance", balance - amount, id);
        return true;
    }
}
