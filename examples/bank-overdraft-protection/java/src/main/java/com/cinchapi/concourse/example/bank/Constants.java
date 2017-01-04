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
import java.sql.DriverManager;
import java.sql.Statement;

import com.cinchapi.concourse.ConnectionPool;
import com.google.common.base.Throwables;

/**
 * A collection of constants that are used throughout the project.
 * 
 * @author Jeff Nelson
 */
public class Constants {

    /**
     * Return the connection to a local MySQL database.
     * 
     * @return the SQL connection
     */
    public static Connection localMySqlConnection() {
        if(MYSQL_CONNECTION == null) {
            try {
                Class.forName("com.mysql.jdbc.Driver");
                MYSQL_CONNECTION = DriverManager.getConnection(
                        "jdbc:mysql://localhost:3306/bank", "root", "");
                // We attempt the create the scheme within the application for
                // demonstration purposes. Obviously, in a real application, the
                // schema would be managed externally and only created/updated
                // once. However, it is important to demonstrate schema creation
                // here to show an example of startup friction when using SQL as
                // opposed to Concourse, which is completely schemaless.
                Connection conn = MYSQL_CONNECTION;
                Statement stmt;
                stmt = conn.createStatement();
                stmt.executeUpdate("CREATE DATABASE IF NOT EXISTS `bank`");
                stmt.executeUpdate("CREATE TABLE IF NOT EXISTS `account` (`id` bigint(20) NOT NULL DEFAULT '0',`balance` double(5,2) DEFAULT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;");
                stmt.executeUpdate("CREATE TABLE IF NOT EXISTS `customer` (`id` bigint(20) NOT NULL DEFAULT '0', `first_name` varchar(200) DEFAULT NULL, `last_name` varchar(200) DEFAULT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;");
                // In order to model many-to-many relationships among
                // records/rows in SQL, you have to create "join tables" with
                // foreign key constrains. Concourse makes this a lot easier
                // since you can link records to one another and store multiple
                // values in a single field.
                stmt.executeUpdate("CREATE TABLE IF NOT EXISTS `account_charge` (`account_id` bigint(20) DEFAULT NULL, `charge` varchar(200) DEFAULT NULL, KEY `account_id` (`account_id`), CONSTRAINT `account_charge_ibfk_1` FOREIGN KEY (`account_id`) REFERENCES `account` (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;");
                stmt.executeUpdate("CREATE TABLE IF NOT EXISTS `account_owner` (`account_id` bigint(20) DEFAULT NULL, `owner` bigint(20) DEFAULT NULL, KEY `account_id` (`account_id`), KEY `owner` (`owner`), CONSTRAINT `account_owner_ibfk_1` FOREIGN KEY (`account_id`) REFERENCES `account` (`id`), CONSTRAINT `account_owner_ibfk_2` FOREIGN KEY (`owner`) REFERENCES `customer` (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;");
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
        return MYSQL_CONNECTION;
    }

    /**
     * Discard the current {@link #CONCOURSE_CONNECTIONS connection pool} and
     * create a new instance to take advantage of changes to the
     * {@code concourse_prefs.file} that is used. This is useful for unit tests
     * that always create a new server instance.
     */
    protected static void refreshConnectionInfo() { // visible for testing
        try {
            if(!CONCOURSE_CONNECTIONS.isClosed()) {
                CONCOURSE_CONNECTIONS.close();
            }
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
        CONCOURSE_CONNECTIONS = ConnectionPool.newCachedConnectionPool();
    }

    /**
     * A {@link ConnectionPool} that provides connections to Concourse on
     * demand. By default, this will connect to the server at
     * <em>localhost:1717</em>, but you can place a concourse_client.prefs file
     * with alternative connection information in the working directory or you
     * can use one of the other factory methods in the {@link ConnectionPool}
     * class to create the connection.
     */
    public static ConnectionPool CONCOURSE_CONNECTIONS = ConnectionPool
            .newCachedConnectionPool();

    /**
     * Singleton for the local MySQL connection.
     */
    private static Connection MYSQL_CONNECTION = null;

}
