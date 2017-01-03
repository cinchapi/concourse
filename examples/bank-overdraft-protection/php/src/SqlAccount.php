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

require_once dirname(__FILE__)."/Databases.php";
require_once dirname(__FILE__)."/Account.php";

/**
 * Implementation of the Account interface that is backed by a SQL database.
 */
class SqlAccount implements Account {

    /**
     * The id of the Concourse record that holds the data for an instance of
     * this class.
     */
    private $id;

    /**
     * @Override
     */
    public function getId(){
        return $this->id;
    }

    /**
     * Construct a new instance.
     *
     * @param double $balance the initial balance
     * @param array $customers the owners of the account
     */
    public function __construct($balance, array $owners){
        $db = Databases::mysql();
        $this->id = current_time_millis();
        // Must use a transaction since we are inserting data into multiple
        // tables. In Concourse, all of this data (links included) are
        // inserted into a single record, so we can take advantage of the
        // fact that inserts are natively atomic.

        // NOTE: should set ISOLATION LEVEL to SERIALIZABLE but i'm not sure how
        // to do that with PDO besides issuing a query directly to the
        // underlying db...
        $db->beginTransaction();
        try {
            $stmt = $db->prepare("INSERT INTO account (id, balance) VALUES (?,?)");
            $stmt->bindParam(":id", $this->id);
            $stmt->bindParam(":balance", $balance);
            $stmt->execute();

            $stmt = $db->prepare("INSERT INTO account_owner (account_id, owner) VALUES (?,?)");
            $stmt->bindParam(":account_id", $this->id);
            foreach($owners as $owner){
                $stmt->bindParam(":owner", $owner->getId());
                $stmt->execute();
            }
            $db->commit();
        }
        catch(Exception $e){
            $db->rollback();
        }
    }

    /**
     * @Override
     */
    public function debit($charge, $amount){
        $db = Databases::mysql();
        $db->beginTransaction();
        try {
            if($this->withdrawImpl($db, $amount)){
                $stmt = $db->prepareStatement("INSERT INTO account_charge (account_id, charge) VALUES (?,?)");
                $stmt->bindParam(":account_id", $this->id);
                $stmt->bindParam(":charge", $chage);
                $stmt->execute();
                $db->commit();
                return true;
            }
            else {
                $db->rollback();
                return false;
            }
        }
        catch(Exception $e){
            $db->rollback();
        }
    }

    /**
     * @Override
     */
    public function deposit($amount){
        $db = Databases::mysql();
        $count = $db->execute("UPDATE account SET balance = balance + $amount WHERE id = ".$this->id);
        return $count == 1;
    }

    /**
     * @Override
     */
    public function getBalance(){
        $db = Databases::mysql();
        $stmt = $db->query("SELECT balance FROM account WHERE id = ".$this->id);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        return $row['balance'];
    }

    /**
     * @Override
     */
    public function getOwners(){
        $db = Databases::mysql();
        $stmt = $db->query("SELECT owner FROM account_owner WHERE account_id = ?");
        $owners = [];
        foreach($stmt->fetchAll(PDO::FETCH_ASSOC) as $row){
            $owners[] = new SqlCustomer($row['owner']);
        }
        return $owners;
    }

    /**
     * @Override
     */
    public function withdraw($amount){
        return $this->debit("withdraw $amount", $amount);
    }

    /**
     * An implementation that withdraws $amount of money from this
     * account using the provided $db connection. This method does
     * not start a new transaction because it assumes that the caller has
     * already done so.
     *
     * @param \PDO $db the connection to the SQL database that is retrieved from
     *                 the Databases class.
     * @param $amount the amount to withdraw
     * @return bool true if the withdrawal is successful (e.g. there is
     *         enough money in the account (possibly after transferring from
     *         other accounts) to withdraw the money without leaving a negative
     *         balance).
     */
    public function withdrawImpl($db, $amount){
        $stmt = $db->query("SELECT balance FROM account where id = ".$this->id);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $balance = $row['balance'];
        $need = $amount - $balance;
        if($need > 0){
            // Get all the other accounts that are owned by the owners of this
            // account
            $sql = "SELECT DISTINCT(account_id) FROM account JOIN account_owner ON account_id = account_owner.account_id WHERE id <> $id AND (";
            $first = true;
            foreach($this->getOwners() as $owner){
                if(!$first){
                    $ccl.=" OR ";
                }
                $first = false;
                $ccl .= "owner = ".$owner->getId();
            }
            $ccl .= ")";
            $stmt = $db->query($sql);
            $results = $stmt->fetchAll(PDO::FETCH_ASSOC);
            foreach($results as $row){
                if($need <= 0){
                    break;
                }
                $toTransfer = $otherBalance > $need ? $need : $otherBalance;
                if($toTransfer > 0 ){
                    $other = $row['account_id'];
                    $stmt = $db->query("SELECT balance FROM account WHERE id = $other");
                    $row = $stmt->fetch(PDO::FETCH_ASSOC);
                    $otherBalance = $row['balance'];
                    $otherBalance -= $toTransfer;
                    $balance += $toTransfer;
                    $db->exec("UPDATE account SET balance = $otherBalance WHERE id = $other");
                    $db->exec("UPDATE account SET balance = $balance WHERE id = ".$this->id);
                    $db->exec("INSERT INTO account_charge (account_id, charge) VALUES ($other, \"transfer $toTransfer to account ".$this->id."\")");
                    $need -= $toTransfer;
                }
            }
            if($need > 0){
                return false;
            }
        }
        $db->exec("UPDATE account set balance = ".$balance - $amount." WHERE id = ".$this->id);
        return true;
    }

}
