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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * An implementation of the {@link Customer} interface that uses a SQL database
 * for data storage.
 * 
 * @author Jeff Nelson
 */
public class SqlCustomer implements Customer {

    /**
     * The primary key for the row that holds the Account.
     */
    private final long id;

    /**
     * Construct a new database record.
     * 
     * @param firstName
     * @param lastName
     */
    public SqlCustomer(String firstName, String lastName) {
        try {
            this.id = System.currentTimeMillis();
            PreparedStatement stmt = Constants
                    .localMySqlConnection()
                    .prepareStatement(
                            "INSERT INTO customer (id, first_name, last_name) VALUES (?,?,?)");
            stmt.setLong(1, id); // for some reason the JDBC/SQL API is not
                                 // 0-indexed like everything else in software
                                 // -_-
            stmt.setString(2, firstName);
            stmt.setString(3, lastName);
            stmt.execute();

        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Load an existing database record.
     * 
     * @param id
     */
    public SqlCustomer(long id) {
        try {
            PreparedStatement stmt = Constants.localMySqlConnection()
                    .prepareStatement(
                            "SELECT count(id) FROM customer WHERE id = ?");
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
    public long getId() {
        return id;
    }

}
