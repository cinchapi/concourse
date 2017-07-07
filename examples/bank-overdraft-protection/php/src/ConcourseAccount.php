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
require_once dirname(__FILE__)."/ConcourseCustomer.php";

/**
 * An implementation of the Account interface that uses Concourse as the backing
 * store.
 */
class ConcourseAccount implements Account {

    /**
     * The id of the Concourse record that holds the data for an instance of
     * this class.
     */
    private $id;

    /**
     * Construct a new instance.
     *
     * @param double $balance the initial balance
     * @param array $customers the owners of the account
     */
    public function __construct($balance, array $customers){
        if($balance > 0){
            $concourse = Databases::concourse();
            $data = [];
            $data['_class'] = get_class();
            $data['balance'] = $balance;
            $data['owners'] = [];
            foreach($customers as $customer){
                $data['owners'][] = \Concourse\Link::to($customer->getId());
            }
            $this->id = $concourse->insert(['data' => $data])[0];
        }
        else{
            throw new Exception('balance must be greater than 0');
        }
    }

    /**
     * @Override
     */
    public function debit($charge, $amount){
        if($amount > 0){
            $concourse = Databases::concourse();
            $concourse->stage();
            try {
                if($this->withdrawImpl($concourse, $amount)){
                    // By using the #add method, we can store multiple charges
                    // in the record at the same time
                    $concourse->add('charges', $charge, $this->id);
                    return $concourse->commit();
                }
                else {
                    $concourse->abort();
                    return false;
                }
            }
            catch(Concourse\TransactionException $e){
                $concourse->abort();
                return false;
            }
        }
        else{
            throw new Exception('balance must be greater than 0');
        }
    }

    /**
     * @Override
     */
    public function deposit($amount){
        // This implementation uses verifyAndSwap to atomically increment the
        // account balance (without a using a transaction!) which ensures that
        // money isn't lost if two people make a deposit at the same time.
        $concourse = Databases::concourse();
        $incremented = false;
        do {
            $balance = $concourse->get(['key' => 'balance', 'id' => $this->id]);
            $newBalance = $balance + $amount;
            if($concourse->verifyAndSwap('balance', $balance, $this->id, $newBalance)){
                $incremented = true;
            }
        }
        while(!incremented);

    }

    /**
     * @Override
     */
    public function getBalance() {
        $concourse = Databases::concourse();
        return Databases::concourse()->get(['key' => 'balance', 'record' => $this->id]);
    }

    /**
     * @Override
     */
    public function getId() {
        return $this->id;
    }

    /**
     * @Override
     */
    public function getOwners() {
        $concourse = Databases::concourse();
        $links = $concourse->select(['key' => 'owners', 'record' => $this->id]);
        $owners = [];
        foreach($owners as $ownerId){
            $owners[] = new ConcourseCustomer($ownerId->getRecord());
        }
        return $owners;
    }

    /**
     * @Override
     */
    public function withdraw($amount) {
        return $this->debit("widthdraw $amount", $amount);
    }

    /**
     * An implementation that withdraws $amount of money from this
     * account using the provided $concourse connection. This method does
     * not start a new transaction because it assumes that the caller has
     * already done so.
     *
     * @param \Concourse $concourse the connection to Concourse that is
     *                              retrieved from the Databases class.
     * @param $amount the amount to withdraw
     * @return bool true if the withdrawal is successful (e.g. there is
     *         enough money in the account (possibly after transferring from
     *         other accounts) to withdraw the money without leaving a negative
     *         balance).
     */
    public function withdrawImpl($concourse, $amount){
        $balance = $concourse->get(['key' => 'balance', 'record' => $this->id]);
        $need = $amount - $balance;
        if($need > 0){
            // Get all the other accounts that are owned by the owners of this
            // account
            $ccl = "_class = ".get_class()." AND (";
            $first = true;
            foreach($this->getOwners() as $owner){
                if(!$first){
                    $ccl.=" OR ";
                }
                $first = false;
                $ccl .= "owners lnks2 ".$owner->getId();
            }
            $ccl .= ")";
            $data = $concourse->get(['key' => 'balance', 'criteria' => $ccl]);
            foreach($data as $record => $otherBalance) {
                if($need <= 0){
                    break;
                }
                else {
                    if($record == $this->id){
                        // Don't try to transfer money from the same account as
                        // this!
                        continue;
                    }
                    $toTransfer = $otherBalance > $need ? $need : $otherBalance;
                    if($toTransfer > 0 ){
                        $otherBalance -= $toTransfer;
                        $balance += $toTransfer;
                        $concourse->set('balance', $balance, $this->id);
                        $concourse->set('balance', $otherBalance, $record);
                        $concourse->add('charges', "transfer $toTransfer to account ".$this->id, $record);
                        $need -= $toTransfer;
                    }
                }
            }
            if($need > 0){
                return false;
            }
        }
        $concourse->set('balance', $balance - $amount, $this->id);
        return true;
    }
}
