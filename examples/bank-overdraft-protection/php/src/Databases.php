<?php
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

namespace Cinchapi\Examples\Bank;

require_once dirname(__FILE__)."/../vendor/autoload.php";

/**
 * A collection database connection singletons.
 *
 * @author Jeff Nelson
 */
 class Databases {

     /**
     * @var \Concourse The singleton for the Concourse connection.
     */
     private static $concourse = null;

     /**
      * @var \PDO The singleton for the MySQL connection.
      */
     private static $mysql = null;

     /**
     * Retrive the connection to Concourse.
     *
     * @return \Concourse the connection to Concourse
     */
     public static function concourse(){
         if(static::$concourse == null){
             static::$concourse = \Concourse::connect();
         }
         return static::$concourse;
     }

     /**
      * Retrieve the connection to MySQL.
      *
      * @return \PDO the connection to mysql
      */
     public static function mysql(){
         if(static::$mysql == null){
             static::$mysql = new \PDO('mysql:host=127.0.0.1;dbname=bank', 'root', '');
             $db = static::$mysql;
             // We attempt the create the schema within the application for
             // demonstration purposes. Obviously, in a real application, the
             // schema would be managed externally and only created/updated
             // once. However, it is important to demonstrate schema creation
             // here to show an example of startup friction when using SQL as
             // opposed to Concourse, which is completely schemaless.
             $db->exec("CREATE DATABASE IF NOT EXISTS `bank`");
             $db->exec("CREATE TABLE IF NOT EXISTS `account` (`id` bigint(20) NOT NULL DEFAULT '0',`balance` double(5,2) DEFAULT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;");
             $db->exec("CREATE TABLE IF NOT EXISTS `customer` (`id` bigint(20) NOT NULL DEFAULT '0', `first_name` varchar(200) DEFAULT NULL, `last_name` varchar(200) DEFAULT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;");
             // In order to model many-to-many relationships among records/rows
             // in SQL, you have to create "join tables" with foreign key
             // constrains. Concourse makes this a lot easier since you can link
             // records to one another and store multiple values in a single
             // field.
             $db->exec("CREATE TABLE IF NOT EXISTS `account_charge` (`account_id` bigint(20) DEFAULT NULL, `charge` varchar(200) DEFAULT NULL, KEY `account_id` (`account_id`), CONSTRAINT `account_charge_ibfk_1` FOREIGN KEY (`account_id`) REFERENCES `account` (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;");
             $db->exec("CREATE TABLE IF NOT EXISTS `account_owner` (`account_id` bigint(20) DEFAULT NULL, `owner` bigint(20) DEFAULT NULL, KEY `account_id` (`account_id`), KEY `owner` (`owner`), CONSTRAINT `account_owner_ibfk_1` FOREIGN KEY (`account_id`) REFERENCES `account` (`id`), CONSTRAINT `account_owner_ibfk_2` FOREIGN KEY (`owner`) REFERENCES `customer` (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;");
         }
         return static::$mysql;
     }
 }
