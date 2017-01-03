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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

/**
 * An implementation of the {@link Account} interface that uses MySQL for data
 * storage.
 * 
 * @author Jeff Nelson
 */
public class SqlAccount implements Account {

    /**
     * The primary key for the row that holds the Account.
     */
    private final long id;

    /**
     * This constructor creates a new record in MySQL and inserts the data
     * expressed in the parameters.
     * 
     * @param balance the initial balance for the account
     * @param owners {@link Customer customers} that are owners on the account
     */
    public SqlAccount(double balance, Customer... owners) {
        Connection conn = Constants.localMySqlConnection();
        try {
            this.id = System.currentTimeMillis();
            // Must use a transaction since we are inserting data into multiple
            // tables. In Concourse, all of this data (links included) are
            // inserted into a single record, so we can take advantage of the
            // fact that inserts are natively atomic.
            conn.setAutoCommit(false);
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            PreparedStatement stmt = conn
                    .prepareStatement("INSERT INTO account (id,balance) VALUES (?,?)");
            stmt.setLong(1, id);
            stmt.setDouble(2, balance);
            stmt.executeUpdate();
            stmt = conn
                    .prepareStatement("INSERT INTO account_owner (account_id, owner) VALUES (?,?)");
            stmt.setLong(1, id);
            for (Customer customer : owners) {
                stmt.setLong(2, customer.getId());
                stmt.executeUpdate();
            }
            conn.commit();
            conn.setAutoCommit(true);
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * This constructor loads an existing object from MySQL.
     * 
     * @param id the primary key for the row that holds the data for the object
     *            we want to load
     */
    public SqlAccount(long id) {
        try {
            PreparedStatement stmt = Constants.localMySqlConnection()
                    .prepareStatement(
                            "SELECT count(id) FROM account WHERE id = ?");
            stmt.setLong(1, id);
            ResultSet results = stmt.executeQuery();
            results.next();
            Preconditions.checkArgument(results.getInt(1) == 1);
            this.id = id;
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public boolean debit(String charge, double amount) {
        Preconditions.checkArgument(amount > 0);
        Connection conn = Constants.localMySqlConnection();
        try {
            conn.setAutoCommit(false);
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            if(withdrawImpl(conn, amount)) {
                PreparedStatement stmt = conn
                        .prepareStatement("INSERT INTO account_charge (account_id, charge) VALUES (?,?)");
                stmt.setLong(1, id);
                stmt.setString(2, charge);
                stmt.executeUpdate();
                conn.commit();
                return true;
            }
            else {
                conn.rollback();
                return false;
            }
        }
        catch (SQLException e) {
            try {
                conn.rollback();
                return false;
            }
            catch (SQLException e2) {
                throw Throwables.propagate(e2);
            }
        }
        finally {
            try {
                conn.setAutoCommit(true);
            }
            catch (SQLException e2) {
                throw Throwables.propagate(e2);
            }
        }
    }

    @Override
    public boolean deposit(double amount) {
        Preconditions.checkArgument(amount > 0);
        Connection conn = Constants.localMySqlConnection();
        try {
            Statement stmt = conn.createStatement();
            int count = stmt
                    .executeUpdate("UPDATE account SET balance = balance + "
                            + amount + " WHERE id = " + id);
            return count == 1;
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public double getBalance() {
        Connection conn = Constants.localMySqlConnection();
        try {
            PreparedStatement stmt = conn
                    .prepareStatement("SELECT balance FROM account WHERE id = ?");
            stmt.setLong(1, id);
            ResultSet results = stmt.executeQuery();
            results.next();
            return results.getDouble(1);
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public Customer[] getOwners() {
        Connection conn = Constants.localMySqlConnection();
        try {
            PreparedStatement stmt = conn
                    .prepareStatement("SELECT owner FROM account_owner WHERE account_id = ?");
            stmt.setLong(1, id);
            ResultSet results = stmt.executeQuery();
            List<Customer> owners = Lists.newArrayList();
            while (results.next()) {
                long cid = results.getLong(1);
                owners.add(new SqlCustomer(cid));
            }
            return owners.toArray(new Customer[0]);
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
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
     * @param conn the connection to MySQL that is retrieved from
     *            {@link Constants#localMySqlConnection()}
     * @param other the recipient {@link SqlAccount account} for the
     *            transferred funds
     * @param amount the amount to transfer from this account.
     * @return the amount of money that is actually transferred from this
     *         account to {@code other}. An account can only transfer as much
     *         money as it has. So, if this account has a balance that is
     *         smaller than {@code amount}, it will transfer only how much it
     *         has.
     */
    private double transferTo(Connection conn, SqlAccount other, double amount) {
        try {
            PreparedStatement stmt = conn
                    .prepareStatement("SELECT balance FROM account WHERE id = ?");
            stmt.setLong(1, id);
            ResultSet results = stmt.executeQuery();
            results.next();
            double balance = results.getDouble(1);
            if(balance > 0) {
                double toTransfer = balance > amount ? amount : balance;
                balance -= toTransfer;
                stmt.setLong(1, other.getId());
                results = stmt.executeQuery();
                results.next();
                double otherBalance = results.getDouble(1);
                otherBalance += toTransfer;
                stmt = conn
                        .prepareStatement("UPDATE account SET balance = ? WHERE id = ?");
                stmt.setDouble(1, balance);
                stmt.setLong(2, id);
                stmt.executeUpdate();
                stmt.setDouble(1, otherBalance);
                stmt.setLong(2, other.getId());
                stmt.executeUpdate();
                stmt = conn
                        .prepareStatement("INSERT INTO account_charge (account_id, charge) VALUES (?,?)");
                stmt.setLong(1, id);
                stmt.setString(2, "transfer " + toTransfer + " to account "
                        + other.getId());
                stmt.executeUpdate();
                return toTransfer;
            }
            else {
                return 0;
            }
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * An implementation that withdraws {@code amount} of money from this
     * account using the provided {@code MySQL} connection. This method does
     * not start a new transaction because it assumes that the caller has
     * already done so.
     * 
     * @param conn the connection to MySQL that is retrieved from
     *            {@link Constants#localMySqlConnection()}
     * @param amount the amount to withdraw
     * @return {@code true} if the withdrawal is successful (e.g. there is
     *         enough money in the account (possibly after transferring from
     *         other accounts) to withdraw the money without leaving a negative
     *         balance).
     */
    private boolean withdrawImpl(Connection conn, double amount) {
        try {
            PreparedStatement stmt = conn
                    .prepareStatement("SELECT balance FROM account WHERE id = ?");
            stmt.setLong(1, id);
            ResultSet results = stmt.executeQuery();
            results.next();
            double balance = results.getDouble(1);
            double need = amount - balance;
            if(need > 0) {
                // Get all the other accounts that are owned by the owners of
                // this account
                StringBuilder sql = new StringBuilder();
                sql.append("SELECT DISTINCT(account_id) from account JOIN account_owner on account.id = account_owner.account_id WHERE account_id <> "
                        + id + " AND (");
                boolean first = true;
                for (Customer customer : getOwners()) {
                    if(!first) {
                        sql.append(" OR ");
                    }
                    first = false;
                    sql.append("owner = " + customer.getId());
                }
                sql.append(")");
                Statement stmt2 = conn.createStatement();
                results = stmt2.executeQuery(sql.toString());
                while (need > 0 && results.next()) {
                    long rid = results.getLong(1);
                    SqlAccount other = new SqlAccount(rid);
                    double transferred = other.transferTo(conn, this, need);
                    balance += transferred;
                    need -= transferred;
                }
                if(need > 0) {
                    return false;
                }
            }
            PreparedStatement stmt3 = conn
                    .prepareStatement("UPDATE account SET balance = ? WHERE id = ?");
            stmt3.setDouble(1, balance);
            stmt3.setLong(2, id);
            int count = stmt.executeUpdate();
            return count == 1;
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

}
