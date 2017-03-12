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

import com.cinchapi.concourse.example.DatabaseObject;

/**
 * An interface for a bank account that defines a few common operations.
 * Implementations should provide overdraft protection where an attempt is made
 * to transfer enough funds from other accounts owned by the
 * {@link #getOwners() owners} of this account to cover a debit or withdrawal in
 * the event that this account does not have enough to cover.
 * 
 * @author Jeff Nelson
 */
public interface Account extends DatabaseObject {

    /**
     * Return the current balance for the account.
     * 
     * @return the current balance
     */
    public double getBalance();

    /**
     * Return an array of {@link Customer} objects that represent the owners of
     * this account.
     * 
     * @return an array of {@link Customer customers} who are owners
     */
    public Customer[] getOwners();

    /**
     * Deduct {@code amount} from the account's balance and record the
     * {@code charge} in the ledger.
     * 
     * @param charge a description of the charge
     * @param amount the amount to deduct
     * @return {@code true} if the debit is successful
     */
    public boolean debit(String charge, double amount);

    /**
     * Withdraw {@code amount} from this account.
     * 
     * @param amount the amout to withdraw
     * @return {@code true} if the withdrawal is successful
     */
    public boolean withdraw(double amount);

/**
     * Deposit {@code amount into this account.
     * @param amount the amount to deposit
     * @return {@code true} if the deposit is successful
     */
    public boolean deposit(double amount);

}
