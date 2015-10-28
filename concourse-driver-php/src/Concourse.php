<?php
/*
 * Copyright 2015 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
require_once dirname(__FILE__) . "/autoload.php";

use Thrift\Transport\TSocket;
use Thrift\Transport\TBufferedTransport;
use Thrift\Protocol\TBinaryProtocolAccelerated;
use Concourse\Thrift\Data\TObject;
use Concourse\Thrift\Shared\TransactionToken;
use Concourse\Thrift\Shared\Type;
use Concourse\Thrift\Shared\AccessToken;
use Concourse\Thrift\Exceptions\TransactionException;
use Concourse\Thrift\ConcourseServiceClient;
use Concourse\Link;
use Concourse\Convert;

/**
 * Concourse is a self-tuning database that is designed for both ad hoc
 * analytics and high volume transactions at scale.
 *
 * This is the main entry point into the PHP Driver for Concourse.
 *
 * Use Concourse::connect() to create a client connection.
*
* @author Jeff Nelson
*/
final class Concourse {

    /**
    * Create a new client connection.
    *
    * @param string $host the server host (optional, defaults to 'localhost')
    * @param integer $port the listener port (optional, defaults to 1717)
    * @param string $username the username with which to connect (optional, defaults to 'admin')
    * @param string $password the password for the username (optional, defaults to 'admin')
    * @param string $environment the environment to use (optional, defaults to the 'value of the default_environment' variable in the server's concourse.prefs file
    * @return Concourse
    * @throws Exception if the connection cannot be established
    */
    public static function connect($host = "localhost", $port = 1717,
    $username = "admin", $password = "admin", $environment = "") {
        return new Concourse($host, $port, $username, $password, $environment);
    }

    /**
     * @var string the server host where the client is connected.
     */
    private $host;

    /**
     * @var integer the server port where the client is connected.
     */
    private $port;

    /**
     * @var string the username on behalf of whom the client is connected.
     */
    private $username;

    /**
     * @var string the password for the $username.
     */
    private $password;

    /**
     * @var string the server environment where the client is connected.
     */
    private $environment;

    /**
     * @var ConcourseServerClient the thrift client
     */
    private $client;

    /**
     * @var Thrift\Shared\TransactionToken the token that identifies the client's server-side transaction. This value is NULL if the client is in autocommit mode.
     */
    private $transaction;

    /**
     * @var Thrift\Shared\AccessToken the access token that is used to identify, authenticate and authorize the client after the initial connection.
     */
    private $creds;

    /**
     * @internal
     * Use Concourse::connect instead of directly calling this constructor.
     */
    private function __construct($host="localhost", $port=1717, $username="admin", $password="admin", $environment="") {
        $kwargs = func_get_arg(0);
        if(is_assoc_array($kwargs)){
            $host = "localhost";
            $prefs = Concourse\find_in_kwargs_by_alias("prefs", $kwargs);
            if(!empty($prefs)){
                $prefs = parse_ini_file(expand_path($prefs));
            }
            else{
                $prefs = [];
            }
        }
        else{
            $kwargs = [];
            $prefs = [];
        }
        // order of precedence for args: prefs -> kwargs -> positional -> default
        $this->host = $prefs["host"] ?: $kwargs["host"] ?: $host;
        $this->port = $prefs["port"] ?: $kwargs["port"] ?: $port;
        $this->username = $prefs["username"] ?: Concourse\find_in_kwargs_by_alias('username', $kwargs) ?: $username;
        $this->password = $prefs["password"] ?: Concourse\find_in_kwargs_by_alias("password", $kwargs) ?: $password;
        $this->environment = $prefs["environment"] ?: $kwargs["environment"] ?: $environment;
        try {
            $socket = new TSocket($this->host, $this->port);
            $transport = new TBufferedTransport($socket);
            $protocol = new TBinaryProtocolAccelerated($transport);
            $this->client = new ConcourseServiceClient($protocol);
            $transport->open();
            $this->authenticate();
        }
        catch (TException $e) {
            throw new Exception("Could not connect to the Concourse Server at "
            . $this->host . ":" . $this->port);
        }
    }

    /**
     * Abort the current transaction and discard any changes that were staged.
     * After returning, the driver will return to autocommit mode and all
     * subsequent changes will be committed immediately.
    */
    public function abort() {
        if(!empty($this->transaction)){
            $token = $this->transaction;
            $this->transaction = null;
            $this->client->abort($this->creds, $token, $this->environment);
        }
    }

    /**
     * Add a value if it doesn't already exist.
     *
     * @api
     ** <strong>add($key, $value, $record)</strong> - Add a value to a field in a single record and return a flag that indicates whether the value was added to the field
     ** <strong>add($key, $value, $records)</strong> - Add a value to a field in multiple records and return a mapping from each record to a boolean flag that indicates whether the value was added to the field
     ** <strong>add($key, $value)</strong> - Add a value to a field in a new record and return the id of the new record
     *
     * @param string $key the field name
     * @param mixed $value the value to add
     * @param integer $record The record where the data should be added (optional)
     * @param array $records The records where the data should be added (optional)
     * @return boolean|array|integer
     * @throws Thrift\Exceptions\InvalidArgumentException
     */
    public function add() {
        return $this->dispatch(func_get_args());
    }

    /**
     * Describe changes made to a record or a field over time.
     *
     * @api
     ** <strong>audit($key, $record)</strong> - Describe all the changes made to a field over time.
     ** <strong>audit($key, $record, $start)</strong> - Describe all the changes made to a field since the specified <em>start</em> timestamp.
     ** <strong>audit($key, $record, $start, $end)</strong> - Describe all the changes made to a field between the specified <em>start</em> and <em>end</em> timestamps.
     ** <strong>audit($record)</strong> - Describe all the changes made to a record over time.
     ** <strong>audit($record, $start)</strong> - Describe all the changes made to a record since the specified <em>start</em> timestamp.
     ** <strong>audit($record, $start, $end)</strong> - Describe all the changes made to a record between the specified <em>start</em> and <em>end</em> timestamps.
     *
     * @param string $key the field name (optional)
     * @param integer $record the record that contains the $key field or the record to audit if no $key is provided
     * @param integer|string $start the earliest timestamp to check (optional)
     * @param integer|string $end the latest timestamp to check (optional)
     * @return array mapping from each timestamp to a description of the change that occurred.
     */
    public function audit(){
        return $this->dispatch(func_get_args());
    }

    /**
     * View the values that have been indexed.
     *
     * @api
     ** <strong>browse($key)</strong> - View that values that are indexed for <em>key</em> and return an array of records where the value is contained in the field.
     ** <strong>browse($key, $timestamp)</strong> - View that values that were indexed for <em>key</em> at <em>timestamp</em> and return an array of records where the value was contained in the field.
     ** <strong>browse($keys)</strong> - View the values that are indexed for each of the <em>keys</em> and return an array mapping each <em>key</em> to an array of records where the value is contained in the field.
     ** <strong>browse($keys, $timestamp)</strong> - View the values that were indexed for each of the <em>keys</em> at <em>timestamp</em> and return an array mapping each <em>key</em> to an arra of records where the value was contained in the field.
     *
     * @param string $key a single field name (optional: either $key or $keys is required)
     * @param string $keys an array of field names (optional: either $key or $keys is required)
     * @param integer|string $timestamp the timestamp to use when browsing the index (optional)
     * @return ArrayAccess
     */
    public function browse(){
        return Convert::phpify($this->dispatch(func_get_args()));
    }

    /**
     * Return a timeseries that shows the state of a field after each change.
     *
     * @api
     ** <strong>chronologize($key, $record)</strong> - Return a timeseries that shows the state of of a field after every change.
     ** <strong>chronologize($key, $record, $start)</strong> - Return a timeseries that shows the state of the field after every change since <em>$start</em>.
     ** <strong>chronologize($key, $record, $start, $end)</strong> - Return a timeseries that shows the state of the field after every change between <em>$start</em> and <em>$end</em>.
     *
     * @param string $key the field name
     * @param integer $record the record that contains the field
     * @param integer|string $start the first timestamp to include in the timeseries (optional)
     * @param integer|String $end the last timestamp to include in the timeseries (optional)
     * @return array mapping a timestamp to an array that contains all the values that were contained in the field at that timestamp
     */
    public function chronologize(){
        return Convert::phpify($this->dispatch(func_get_args()));
    }

    /**
     * Atomically remove all the values from one or more fields or one or more records.
     *
     * @api
     ** <strong>clear($key, $record)</strong> - Atomically remove all the values from a field in a single record.
     ** <strong>clear($key, $records)</strong> - Atomically remove all the values from a field in multiple records.
     ** <strong>clear($keys, $record)</strong> - Atomically remove all the values from multiple fields in a single record.
     ** <strong>clear($keys, $records)</strong> - Atomically remove all the values from multiple fields in multiple records.
     ** <strong>clear($record)</strong> - Atomically remove all the values from a single record.
     ** <strong>clear($records)</strong> - Atomically remove all the values from multiple records.
     *
     * @param string $key the name of the field to clear
     * @param array $keys a collection of fields to clear
     * @param integer $record the record that contains the field/s or itself in entirety to clear
     * @param array $records a collection of records that contain the field/s or  themselves in entirety to clear
     */
    public function clear(){
        $this->dispatch(func_get_args());
    }

    /**
     * Close the client connection.
     */
    public function close(){
        $this->client->logout($this->creds, $this->environment);
        $this->transport->close();
    }

    /**
     * Commit the currently running transaction.
     * @return boolean that indicates if the transaction successfully committed
     * @throws Cinchapi\Concourse\Thrift\Exceptions\TransactionException
     */
    public function commit(){
        $token = $this->transaction;
        $this->transaction = null;
        if(!is_null($token)){
            return $this->client->commit($this->creds, $token, $this->environment);
        }
        else {
            return false;
        }
    }

    /**
     * Describe the fields that exist.
     *
     * @api
     ** <strong>describe($record)</strong> - Return all the keys in the <em>record</em>.
     ** <strong>describe($record, $timestamp)</strong> - Return all the keys that were in the <em>record</em> at <em>timestamp</em>.
     ** <strong>describe($records)</strong> - Return an array mapping each of the <em>records</em> to the array of keys that are in each record.
     ** <strong>describe($records, $timestamp)</strong> - Return an array mapping each of the <em>records</em> to the array of keys that were in each record at <em>timestamp</em>.
     *
     * @param integer $record the single record to describe (either $record or $records is required)
     * @param array $records the collection of records to describe (either $record or $records is required)
     * @param integer|string $timestamp the timestamp to use when describe the record/s (optional)
     * @return array
     */
    public function describe(){
        return $this->dispatch(func_get_args());
    }

    /**
     * Return the differences in data between two timestamps.
     *
     * @api
     ** <strong>diff($key, $record, $start)</strong> - Return the differences in the field between the <em>start</em> timestamp and the present.
     ** <strong>diff($key, $record, $start, $end)</strong> - Return the differences in the field between the <em>start</em> and <em>end</em> timestamps.
     ** <strong>diff($key, $start)</strong> - Return the differences in the index between the <em>start</em> timestamp and the present.
     ** <strong>diff($key, $start, $end)</strong> - Return the differences in the index between the <em>start</em> and <em>end</em> timestamps.
     ** <strong>diff($record, $start)</strong> - Return the differences in the record between the <em>start</em> timestamp and the present.
     ** <strong>diff($record, $start, $end)</strong> - Return the differences in the record between the <em>start</em> and <em>end</em> timestamps.
     *
     * @param string $key the field or index name
     * @param integer $record the record that contains the field or the record to diff
     * @param integer|string $start the timestamp of the original state
     * @param integer|string $end the timestamp of the changed state
     * @return array mapping a description of a changed (ADDED or REMOVED) to an array of values that match the change
     */
    public function diff(){
        return Convert::phpify($this->dispatch(func_get_args()));
    }

    /**
     * Find the records that match a criteria.
     *
     * @api
     ** <strong>find($key, $operator, $value)</strong> - Find the records where the <em>key</em> field contains at least one value that satisfies <em>operator</em> in relation to <em>value</em>.
     ** <strong>find($key, $operator, $values)</strong> - Find the records where the <em>key</em> field contains at least one value that satisfies <em>operator</em> in relation to the <em>values</em>.
     ** <strong>find($timestamp, $key, $operator, $value)</strong> - Find the records where the <em>key</em> field contained at least one value that satisifed <em>operator</em> in relation to <em>value</em> at <em>timestamp</em>.
     ** <strong>find($timestamp, $key, $operator, $values)</strong> - Find the records where the <em>key</em> field contained at least one value that satisifed <em>operator</em> in relation to the <em>values</em> at <em>timestamp</em>.
     ** <strong>find(criteria)</strong> - Find the records that match the <em>criteria</em>.
     *
     * @param string $key the field/index name
     * @param array $keys the collection of field/index names
     * @param Cinchapi\Concourse\Thrift\Shared\Operator|string $operator The criteria operator
     * @param mixed $value the criteria value
     * @param mixed $values the criteria values
     * @param integer|string $timestamp the timestamp to use when evaluating the criteria
     * @return array the records that match
     */
    public function find(){
        return $this->dispatch(func_get_args());
    }

    /**
     * Find and return the unique record where the <em>key</em> equals
     * <em>value</em>, if it exists. If no record matches, then add <em>key</em>
     * as <em>value</em> in a new record and return the id. If multiple records match the condition, a DuplicateEntryException is thrown.
     *
     * This method can be used to simulate a unique index because it atomically
     * checks for a condition and only adds data if the condition isn't
     * currently satisified. If you want to simulate a unique compound index,
     * see the #findOrInsert method, which lets you check a complex criteria.
     *
     * @param string $key the field name
     * @param mixed $value the value to find for <em>key</em> or add in a new record
     * @return integer The unique record where <em>key</em> = <em>value</em>, if it exists or the new record where <em>key</em> as <em>value</em> is added.
     * @throws Thrift\Exceptions\DuplicateEntryException
     */
    public function findOrAdd(){
        list($args, $kwargs) = Concourse\gather_args_and_kwargs(func_get_args());
        list($key, $value) = $args;
        $key = $key ?: $kwargs['key'];
        $value = $value ?: $kwargs['value'];
        $value = Convert::phpToThrift($value);
        return $this->client->findOrAddKeyValue($key, $value, $this->creds, $this->transaction, $this->environment);
    }

    /**
     * Find and return the unique record that matches the <em>criteria</em>, if
     * it exists. If no record matches, then insert <em>data</em> in a new
     * record and return the id. If multiple records match the
     * <em>criteria</em>, a DuplicateEntryException is thrown.
     *
     * This method can be used to simulate a unique index because it atomically
     * checks for a condition and only adds data if the condition isn't
     * currently satisified.
     *
     * @param string $criteria the unique criteria to find
     * @param mixed $data the data to insert if the criteria is not uniquely found
     * @return integer The unique record that matches the <em>criteria</em>, if it exists or the new record where <em>data</em> is inserted.
     * @throws Thrift\Exceptions\DuplicateEntryException
     */
    public function findOrInsert(){
        list($criteria, $data) = func_get_args();
        if(is_assoc_array($criteria)){
            // Assume using kwargs
            $criteria = $kwargs['criteria'];
            $criteria = $criteria ?: Concourse\find_in_kwargs_by_alias('criteria', $kwargs);
            $data = $kwargs['data'];
            $data = $data ?: $kwargs['json'];
        }
        if(!is_string($data)){
            $data = json_encode($data);
        }
        return $this->client->findOrInsertCclJson($criteria, $data, $this->creds, $this->transaction, $this->environment);
    }

    /**
     * Get the most recently added value/s.
     *
     * @api
     ** <strong>get($criteria)</strong> - Return the most recently added value from all the fields in every record that matches the <em>criteria</em> as array[record => array[key => value]].
     ** <strong>get($criteria, $timestamp)</strong> - Return the most recently added value from all the fields at <em>timestamp</em> in every record at that matches the <em>criteria</em> as array[record => array[key => value]].
     ** <strong>get($key, $criteria)</strong> - Return the most recently added value from the <em>key</em> field in every record that matches the <em>criteria</em> as array[record => value].
     ** <strong>get($key, $criteria, $timestamp)</strong> - Return the most recently added value from the <em>key</em> field at <em>timestamp</em> in every record that matches the <em>criteria</em>. as an array[record => value].
     ** <strong>get($keys, $criteria)</strong> - Return the most recently added value from each of the <em>keys</em> fields in every record that matches the <em>criteria</em> as array[record => array[key => value]].
     ** <strong>get($keys, $criteria, $timestamp)</strong> - Return the most recently added value from each of the <em>keys</em> fields at <em>timestamp</em> from every record that matches the <em>criteria</em> as array[record => array[key => value]].
     ** <strong>get($key, $record)</strong> - Return the most recently added value from the <em>key</em> field in <em>record</em>.
     ** <strong>get($key, $record, $timestamp)</strong> - Return the most recently added value from the <em>key</em> field in <em>record</em> at <em>timestamp</em>.
     ** <strong>get($keys, $record)</strong> - Return the most recently added value from each of the <em>keys</em> fields in <em>record</em> as array[key => value].
     ** <strong>get($keys, $record, $timestamp)</strong> - Return the most recently added value from each of the <em>keys</em> fields in <em>record</em> at <em>timestamp</em> as array[key => value].
     ** <strong>get($keys, $records)</strong> - Return the most recently added value from each of the <em>keys</em> fields in each of the <em>records</em> as array[record => array[key => value]].
     ** <strong>get($keys, $records, $timestamp)</strong> - Return the most recently added values from each of the <em>keys</em> fields in each of the <em>records</em> at <em>timestamp</em> as array[record => array[key => value]].
     *
     * @param string $key the field name
     * @param array $keys the collection of multiple field names
     * @param string $criteria the criteria that determines the record from which data is retrieved
     * @param integer $record the record from which data is retrieved
     * @param array $records the collection of muliple records from which data is retrieved
     * @param integer|string $timestamp the timestamp to use when getting data
     * @return mixed
     */
    public function get(){
        return Convert::phpify($this->dispatch(func_get_args()));
    }

    /**
     * Return the environment to which the client is connected.
     *
     * @return string the server environment associated with this connection
     */
    public function getServerEnvironment(){
        return $this->client->getServerEnvironment($this->creds, $this->transaction, $this->environment);
    }

    /**
     * Return the version of Concourse Server to which the client is
     * connected. Generally speaking, a client cannot talk to a newer version of
     * Concourse Server.
     *
     * @return string the server version
     */
    public function getServerVersion(){
        return $this->client->getServerVersion();
    }

    /**
     * Atomically bulk insert data. This operation is atomic within each record,
     * which means that an insert will only succeed in a record if all the data
     * can be successfully added. Therefore, an insert will fail in a record if
     * any of the insert data is already contained.
     *
     * @api
     ** <strong>insert($data)</strong> - Insert <em>data</em> into one or more new records and return an array of all the new record ids.
     ** <strong>insert($data, $record)</strong> - Insert <em>data</em> into <em>record</em> and return <em>true</em> if the operation is successful.
     ** <strong>insert($data, $records)</strong> - Insert <em>data</em> into each of the <em>records</em> and return an array mapping each record id ot a boolean flag that indicates if the insert was sucessful in that record.
     *
     * @param mixed $data the data to insert
     * @param integer $record the record into which the data is inserted
     * @param array $records the records into which the data is inserted
     * @return array|boolean
     */
    public function insert(){
        return $this->dispatch(func_get_args());
    }

    /**
     * Return all the records that have current or historical data.
     *
     * @return array all the record ids
     */
    public function inventory(){
        return $this->client->inventory($this->creds, $this->transaction, $this->environment);
    }

    /**
     * Export data as a JSON string.
     *
     * @api
     ** <strong>jsonify($record)</strong> - Return a JSON string that contains all the data in <em>record</em>.
     ** <strong>jsonify($record, $timestamp)</strong> - Return a JSON string that contains all the data in <em>record</em> at <em>timestamp</em>.
     ** <strong>jsonify($record, $includeId)</strong> - Return a JSON string that contains all the data in <em>record</em> and optionally include the record id  in the dump. This option is useful for dumping data from one instance and importing into another.
     ** <strong>jsonify($record, $timestamp, $includeId)</strong> - Return a JSON string that contains all the data in <em>record</em> at <em>timestamp</em> and optionally include the record id  in the dump. This option is useful for dumping data from one instance and importing into another.
     ** <strong>jsonify($records)</strong> - Return a JSON string that contains all the data in each of the <em>records</em>.
     ** <strong>jsonify($records, $timestamp)</strong> - Return a JSON string that contains all the data in each of the <em>records</em> at <em>timestamp</em>.
     ** <strong>jsonify($records, $includeId)</strong> - Return a JSON string that contains all the data in each of the <em>records</em> and optionally include the record id  in the dump. This option is useful for dumping data from one instance and importing into another.
     ** <strong>jsonify($records, $timestamp, $includeId)</strong> - Return a JSON string that contains all the data in each of the <em>records</em> at <em>timestamp</em> and optionally include the record id  in the dump. This option is useful for dumping data from one instance and importing into another.
     *
     * @param integer $record  the id of the record to dump
     * @param array $records  an array containing the ids of all the records to dump
     * @param integer|string $timestamp  the timestamp to use when selecting the data to dump
     * @param boolean $includeId  a flag that determines whether record ids are included in the data dump
     * @return string the data encoded as a JSON string
     */
    public function jsonify(){
        list($args, $kwargs) = Concourse\gather_args_and_kwargs(func_get_args());
        list($records, $timestamp, $includeId) = $args;
        $records = $records ?: $kwargs['records'];
        $records = $records ?: $kwargs['record'];
        $records = !is_array($records) ? array($records) : $records;
        $includeId = $includeId ?: $kwargs['includeId'];
        $includeId = $includeId ?: false;
        $timestamp = $timestamp ?: $kwargs['timestamp'];
        $timestamp = $timestamp ?: Concourse\find_in_kwargs_by_alias('time', $kwargs);
        $timestr = is_string($timestamp);
        if(empty($timestamp)) {
            return $this->client->jsonifyRecords($records, $includeId, $this->creds, $this->transaction, $this->environment);
        }
        else if(!empty($timestamp) && !$timestr) {
            return $this->client->jsonifyRecordsTime($records, $timestamp, $includeId, $this->creds, $this->transaction, $this->environment);
        }
        else if(!empty($timestamp) && $timestr) {
            return $this->client->jsonifyRecordsTimestr($records, $timestamp, $includeId, $this->creds, $this->transaction, $this->environment);
        }
        else {
            Concourse\require_arg('record(s)');
        }
    }

    /**
     * Add a link from a field in the <em>source</em> to one or more <em>destination</em> records.
     *
     * @api
     ** <strong>link($key, $source, $destination)</strong> - Add a link from the <em>key</em> field in the <em>source</em> record to the <em>destination</em> record.
     ** <strong>link($key, $source, $destinations)</strong> - Add a link from the <em>key</em> field in the <em>source</em> record to the each of the  <em>destinations</em>.
     *
     * @param string $key the field name
     * @param integer $source the source record
     * @param integer $destination the destination record
     * @param array $destinations the destination records
     * @return boolean|array
     */
    public function link(){
        list($args, $kwargs) = Concourse\gather_args_and_kwargs(func_get_args());
        list($key, $source, $destinations) = $args;
        $key = $key ?: $kwargs['key'];
        $source = $source ?: $kwargs['source'];
        $destinations = $destinations ?: $kwargs['destinations'];
        $destinations = $destinations ?: $kwargs['destination'];
        if(!empty($key) && !empty($source) && is_array($destinations)) {
            $data = [];
            foreach($destinations as $destination){
                $data[$destination] = $this->add($key, Link::to($destination), $source);
            }
            return $data;
        }
        else if(!empty($key) && !empty($source) && is_integer($destinations)) {
            return $this->add($key, Link::to($destinations), $source);
        }
        else {
            Concourse\require_arg("key, source and destination(s)");
        }
    }

    /**
     * @ignore
     * An internal method that allows unit tests to "logout" from the server
     * without closing the transport. This should only be used in unit tests
     * that are connected to Mockcourse.
     */
    public function logout(){
        $this->client->logout($this->creds, $this->environment);
    }

    /**
     * Check if data currently exists.
     *
     * @api
     ** <strong>ping($record)</strong> - Return a boolean that indicates whether the <em>record</em> has any data.
     ** <strong>ping($records)</strong> - Return an arry that maps each of the <em>records</em> to a boolean that indicates whether the record currently has any data.
     *
     * @param integer $record the record to ping
     * @param array $records the records to ping
     * @return boolean|array
     */
    public function ping(){
        return $this->dispatch(func_get_args());
    }

    /**
     * Remove a value if it exists.
     *
     * @api
     ** <strong>remove($key, $value, $record)</strong> - Remove a value from a field in a single record.
     ** <strong>remove($key, $value, $records)</strong> - Remove a value from a field in multiple records.
     *
     * @param string $key the field name
     * @param mixed $value the value to remove from the field
     * @param integer $record the record that contains the field
     * @param array $records the records that contain the field
     * @return boolean|array
     */
    public function remove(){
        return $this->dispatch(func_get_args());
    }

    /**
     * Atomically return data to a previous state.
     *
     * @api
     ** <strong>revert($key, $record, $timestamp)</strong> - Revert the <em>key</em> in <em>record</em> to its state at <em>timestamp</em>.
     ** <strong>revert($keys, $record, $timestamp)</strong> - Revert each of the <em>keys</em> in <em>record</em> to their state at <em>timestamp</em>.
     ** <strong>revert($key, $records, $timestamp)</strong> - Revert the <em>key</em> in each of the <em>records</em> to its state at <em>timestamp</em>.
     ** <strong>revert($keys, $records, $timestamp)</strong> - Revert each of the <em>keys</em> in each of the <em>records</em> to their state at <em>timestamp</em>.
     *
     * @param string $key the field name
     * @param array $keys the field names
     * @param integer $record the record that contains the field/s
     * @param array $records the records that contain the field/s
     * @param integer|string $timestamp the timestamp of the state to restore
     */
    public function revert(){
        $this->dispatch(func_get_args());
    }

    /**
     * Search for all the records that have at a value in the <em>key</em> field that fully or partially matches the <em>query</em>.
     *
     * @param string $key the field name
     * @param string $query the search query to match
     * @return array the records that match
     */
    public function search(){
        list($args, $kwargs) = Concourse\gather_args_and_kwargs(func_get_args());
        list($key, $query) = $args;
        $key = $key ?: $kwargs['key'];
        $query = $query ?: $kwargs['query'];
        if(is_string($key) && is_string($query)) {
            return $this->client->search($key, $query, $this->creds, $this->transaction, $this->environment);
        }
        else {
            Concourse\require_arg('key and query');
        }
    }

    /**
     * Select all values.
     *
     * @api
     ** <strong>select($criteria)</strong> - Return all the data from every record that matches the <em>criteria</em> as an Array[record => Array[key => Array[values]]].
     ** <strong>select($criteria, $timestamp)</strong> - Return all the data at <em>timestamp</em> from every record that matches the <em>criteria</em> as an Array[record => Array[key => Array[values]]].
     ** <strong>select($key, $criteria)</strong> - Return all the values from <em>key</em> in all the records that match the <em>criteria</em> as an Array[record => Array[values]].
     ** <strong>select($key, $criteria, $timestamp)</strong> - Return all the values from <em>key</em> at <em>timestamp</em> in all the records that match the <em>criteria</em> as an Array[record => Array[values]].
     ** <strong>select($keys, $criteria)</strong> - Return all the values from each of the <em>keys</em> in all the records that match <em>criteria</em> as an Array[record => Array[key => Array[values]]].
     ** <strong>select($keys, $criteria, $timestamp)</strong> - Return all the values from each of the <em>keys</em> at <em>timestamp</em> in all the records that match <em>criteria</em> as an Array[record => Array[key => Array[values]]].
     ** <strong>select($key, $record)</strong> - Return all the values from <em>key</em> in <em>record</em>.
     ** <strong>select($key, $record, $timestamp)</strong> - Return all the values from <em>key</em> in <em>record</em> at <em>timestamp</em>.
     ** <strong>select($keys, $record)</strong> - Return all the values from each of the <em>keys</em> in <em>record</em> as an Array[key => Array[values]].
     ** <strong>select($keys, $record, $timestamp)</strong> - Return all the values from each of the <em>keys</em> in <em>record</em> at <em>timestamp</em> as an Array[key => Array[values]].
     ** <strong>select($keys, $records)</strong> - Return all the values from each of the <em>keys</em> in each of the <em>records/em> as an Array[record => Array[key => Array[values]]].
     ** <strong>select($key, $records, $timestamp)</strong> - Return all the values from each of the <em>keys</em> in each of the <em>records/em> at <em>timestamp</em> as an Array[record => Array[key => Array[values]]].
     ** <strong>select($record)</strong> - Return all the values from every key in <em>record</em> as an Array[key => Array[values]].
     ** <strong>select($record, $timestamp)</strong> - Return all the values from every key in <em>record</em> at <em>timestamp</em> as an Array[key => Array[values]].
     ** <strong>select($records)</strong> - Return all the values from every key in each of the <em>records</em> as an Array[record => Array[key => Array[values]]].
     ** <strong>select($records, $timestamp)</strong> - Return all the values from every key in each of the <em>records</em> at <em>timestamp</em> as an Array[record => Array[key => Array[values]]].
     *
     * @param string $criteria a CCL filter that determines from which records to select data (required if $record and $records is unspecified)
     * @param string $key a key from which to select values (optional)
     * @param string[] $keys a collection of multiple keys from which to select values (optional)
     * @param integer $record a record from which to select data (required if $criteria and $records is unspecified)
     * @param integer[] $records a collection of multiple keys from which to select data (required if $criteria and $record is unspecified)
     * @param integer|string $timestamp the timestamp to use when selecting data (optional)
     * @return array
     */
    public function select(){
        return Convert::phpify($this->dispatch(func_get_args()));
    }

    /**
     * Atomically remove all existing values from a field and add a new one.
     *
     * @api
     ** <strong>set($key, $value, $record)</strong> - Atomically remove all the values from <em>key</em> in <em>record</em> and add <em>value</em>.
     ** <strong>set($key, $value, $records)</strong> - Atomically remove all the values from <em>key</em> in each of the <em>records</em> and add <em>value</em>.
     ** <strong>set($key, $value)</strong> - Add <em>key</em> as <em>value</em> in a new record and return the id.
     *
     * @param string $key the field name
     * @param mixed $value the value to add to the field
     * @param integer $record the record in which to set $value for $key (optional)
     * @param integer[] $records a collection of records in which to set $value for $key (optional)
     * @return void|integer
     */
    public function set(){
        return $this->dispatch(func_get_args());
    }

    /**
    * Start a new transaction.
    *
    * This method will turn on <em>staging</em> mode so that all subsequent
    * changes are collected in an isolated buffer before possibly being
    * committed. Staged operations are guaranteed to be reliable, all or
    * nothing, units of work that allow correct recovery from failures and
    * provide isolation between clients so the database is always consistent.
    *
    * After this method returns, all subsequent operations will be done in
    * <em>staging<em> mode until either #commit or #abort is called.
    *
    * All operations that occur within a transaction should be wrapped in a
    * try-catch block so that transaction exceptions can be caught and the
    * application can decided to abort or retry the transaction:
    *
    * <code>
    * 	$concourse->stage();
    * 	try {
    * 		$concourse->get(["key" => "name", "record" => 1]);
    * 		$concourse->add("name", "Jeff Nelson", 1);
    * 		$concourse->commit();
    * 	}
    * 	catch(Cinchapi\Concourse\Thrift\Exceptions\TransactionException $e) {
    * 		$concourse->abort();
    * 	}
    * </code>
    *
    * Alternatively, if you supply a block to this method, starting and
    * committing the transactions happens automatically and there is also
    * automatic logic to gracefully handle exceptions that may result from
    * any of the actions in the transaction.
    *
    * <code>
    * 	$concourse->stage(function() use($concourse){
    * 		$concourse->get(["key" => "name", "record" => 1]);
    * 		$concourse->add("name", "Jeff Nelson", 1);
    * 	});
    * </code>
    */
    public function stage($lambda = null){
        if(is_callable($lambda)){
            $this->commit();
            try {
                $lambda();
                $this->commit();
            }
            catch(TransactionException $e){
                $this->abort();
                throw $e;
            }
        }
        else {
            $this->transaction = $this->client->stage($this->creds, $this->environment);
        }
    }

    /**
     * Return the server's unix timestamp in microseconds. The precision of the timestamp is subject to network latency.
     *
     * @param string $phrase a natural language phrase that describes the desired timestamp (i.e. 3 weeks ago, last month, yesterday at 3:00pm, etc) (optional)
     * @return integer a unix timestamp in microseconds
     */
    public function time(){
        return $this->dispatch(func_get_args());
    }

    /**
     * Remove the link from a key in the <em>source</em> to one or more <em>destination</em> records.
     *
     * @api
     ** <strong>unlink($key, $source, $destination)</strong> - Remove the link from the <em>key</em> in the <em>source</em> record to the <em>destination</em> record.
     ** <strong>unlink($key, $source, $destinations)</strong> - Remove the link from the <em>key</em> in the <em>source</em> record to the each of the  <em>destinations</em>.
     *
     * @param string $key the field name
     * @param integer $source the source record
     * @param integer $destination the destination record (required if $destinations is unspecified)
     * @param array $destinations the destination records (required if $destination is unspecified)
     * @return boolean|array
     */
    public function unlink(){
        list($args, $kwargs) = Concourse\gather_args_and_kwargs(func_get_args());
        list($key, $source, $destinations) = $args;
        $key = $key ?: $kwargs['key'];
        $source = $source ?: $kwargs['source'];
        $destinations = $destinations ?: $kwargs['destinations'];
        $destinations = $destinations ?: $kwargs['destination'];
        if(!empty($key) && !empty($source) && is_array($destinations)) {
            $data = [];
            foreach($destinations as $destination){
                $data[$destination] = $this->remove($key, Link::to($destination), $source);
            }
            return $data;
        }
        else if(!empty($key) && !empty($source) && is_integer($destinations)) {
            return $this->remove($key, Link::to($destinations), $source);
        }
        else {
            Concourse\require_arg("key, source and destination(s)");
        }
    }

    public function verify(){
        return $this->dispatch(func_get_args());
    }

    /**
     * Atomically verify the existence of a <em>value</em> for a <em>key</em>
     * within a <em>record</em> and swap that value with a <em>replacement</em>
     * one.
     *
     * @param string $key the field name
     * @param mixed $value the value for which the verify is performed
     * @param integer $record the record that contains the field
     * @param mixed $replacement the value to for which the original is swapped
     * @return boolean <em>true</em> if and only if both the verification and swap are successful
     */
    public function verifyAndSwap(){
        list($args, $kwargs) = Concourse\gather_args_and_kwargs(func_get_args());
        list($key, $expected, $record, $replacement) = $args;
        $key = $key ?: $kwargs['key'];
        $expected = $expected ?: $kwargs['expected'];
        $expected = $expected ?: Concourse\find_in_kwargs_by_alias('expected', $kwargs);
        $replacement = $replacement ?: $kwargs['replacement'];
        $replacement = $replacement ?: Concourse\find_in_kwargs_by_alias('replacement', $kwargs);
        $expected = Convert::phpToThrift($expected);
        $replacement = Convert::phpToThrift($replacement);
        if(!empty($key) && !empty($expected) && !empty($record) && !empty($replacement)) {
            return $this->client->verifyAndSwap($key, $expected, $record, $replacement, $this->creds, $this->transaction, $this->environment);
        }
        else {
            Concourse\require_arg('key, expected, record, and replacement');
        }

    }

    /**
     * Atomically verify that a field contains a single particular value or set
     * it as such.
     *
     * Please note that after returning, this method guarantees that
     * <em>key</em> in <em>record</em> will only contain <em>value</em>, even if
     * it already existed alongside other values (e.g. calling
     * concourse.verifyOrSet("foo", "bar", 1) will mean that "foo" in record 1
     * will only have "bar" as a value after returning, even if the field
     * already contained "bar", "baz" and "apple" as values.
     *
     * Basically, this method has the same guarantee as the [#set] method,
     * except it will not create any new revisions unless it is necessary to do
     * so. The [#set] method, on the other hand, would indiscriminately clear
     * all the values in the field before adding <em>value</em>, even if it
     * already existed.
     *
     * If you want to add a value that does not exist, while also preserving
     * other values that also exist in the field, you should use the [#add]
     * method instead.
     *
     * @param string $key the field name
     * @param mixed $value the value to ensure exists alone in the field
     * @param integer $record the record that contains the field
     */
    public function verifyOrSet(){
        list($args, $kwargs) = Concourse\gather_args_and_kwargs(func_get_args());
        list($key, $value, $record) = $args;
        $key = $key ?: $kwargs['key'];
        $value = $value ?: $kwargs['value'];
        $record = $record ?: $kwargs['record'];
        $value = Convert::phpToThrift($value);
        $this->client->verifyOrSet($key, $value, $record, $this->creds, $this->transaction, $this->environment);
    }

    /**
     * @Override
     */
    public function __toString(){
        return "Connected to $host:$port as $username";
    }

    /**
    * Login with the username and password and locally store the AccessToken
    * to use with subsequent CRUD methods.
    *
    * @throws Thrift\Exceptions\SecurityException
    */
    private function authenticate() {
        try {
            $this->creds = $this->client->login($this->username, $this->password,
            $this->environment);
        }
        catch (TException $e) {
            throw e;
        }
    }

    /**
     * When called from a method within this class, dispatch to the appropriate
     * thrift callable based on the arguments that are passed in.
     *
     * Usage:
     * return $this->dispatch(func_get_args());
     * @return mixed
     */
    private function dispatch(){
        $args = func_get_args()[0];
        $end = count($args) - 1;
        if(is_assoc_array($args[$end])){
            $kwargs = $args[$end];
            unset($args[$end]);
        }
        else{
            $kwargs = array();
        }
        $method = debug_backtrace()[1]['function'];
        $tocall = Concourse\Dispatcher::send($method, $args, $kwargs);
        $callback = array($this->client, array_keys($tocall)[0]);
        $params = array_values($tocall)[0];
        $params[] = $this->creds;
        $params[] = $this->transaction;
        $params[] = $this->environment;
        return call_user_func_array($callback, $params);
    }

}
