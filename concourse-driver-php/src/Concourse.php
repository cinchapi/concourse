<?php
/*
 * Copyright 2015 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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
     * @param string $username the username with which to connect (optional,
     * defaults to 'admin')
     * @param string $password the password for the username (optional,
     * defaults to 'admin')
     * @param string $environment the environment to use (optional, defaults
     * to the 'value of the default_environment' variable in the server's
     * concourse.prefs file
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
     * @var Thrift\Shared\TransactionToken the token that identifies the
     * client's server-side transaction. This value is NULL if the client is
     * in autocommit mode.
     */
    private $transaction;

    /**
     * @var Thrift\Shared\AccessToken the access token that is used to
     * identify, authenticate and authorize the client after the initial
     * connection.
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
     * Abort the current transaction and discard any changes that are
     * currently staged.
     *
     * After returning, the driver will return to <em>autocommit</em> mode and
     * all subsequent changes will be committed immediately.
     *
     * Calling this method when the driver is not in <em>staging</em> mode is a
     * no-op.
     */
    public function abort() {
        if(!empty($this->transaction)){
            $token = $this->transaction;
            $this->transaction = null;
            $this->client->abort($this->creds, $token, $this->environment);
        }
    }

    /**
     * Append a <em>key</em> as a <em>value</em> in one or more records.
     *
     * @api
     ** <strong>add($key, $value)</strong> - Append <em>key</em> as
     * <em>value</em> in a new record and return the id.
     ** <strong>add($key, $value, $record)</strong> - Append <em>key</em> as
     * <em>value</em> in <em>record</em> if and only if it doesn't exist and
     * return a boolean that indicates if the data was added.
     ** <strong>add($key, $value, $records)</strong> - Atomically Append
     * <em>key</em> as <em>value</em> in each of the <em>records</em>
     * where it doesn't exist and return an associative array associating
     * each record id to a boolean that indicates if the data was added
     *
     * @param string $key the field name
     * @param mixed $value the value to add
     * @param integer $record the record id where an attempt is made
     * to add the data (optional)
     * @param array $records a collection of record ids where an attempt
     * is made to add the data (optional)
     * @return boolean|array|integer
     * @throws Concourse\Thrift\Exceptions\InvalidArgumentException
     */
    public function add() {
        return $this->dispatch(func_get_args());
    }

    /**
     * List changes made to a <em>field</em> or <em>record</em> over time.
     *
     * @api
     ** <strong>audit($key, $record)</strong> - Return a list of all the
     * changes ever made to the <em>key</em> field in <em>record</em>.
     ** <strong>audit($key, $record, $start)</strong> - Return a list of
     * all the changes made to the <em>key</em> field in
     * <em>record</em> since <em>start</em> (inclusive).
     ** <strong>audit($key, $record, $start, $end)</strong> - Return a list
     * of all the changes
     * made to the <em>key</em> field in <em>record</em> between
     * <em>start</em> (inclusive) and <em>end</em> (non-inclusive).
     ** <strong>audit($record)</strong> - Return a list of all the changes
     * ever made to <em>record</em>.
     ** <strong>audit($record, $start)</strong> - Return a list of all the
     *  changes made to <em>record</em> since <em>start</em> (inclusive).
     ** <strong>audit($record, $start, $end)</strong> - Return a list of
     * all the changes made to <em>record</em> between <em>start</em>
     * (inclusive) and <em>end</em> (non-inclusive).
     *
     * @param string $key the field name
     * @param integer $record the record id
     * @param integer|string $start an inclusive <em>timestamp </em> of the
     * oldest change that should possibly be included in the audit -
     * represented as either a natural language description of a point in time
     * (i.e. two weeks ago), OR a number of microseconds since the Unix epoch
     * @param integer|string $end a non-inclusive <em>timestamp</em> for the
     * most recent change that should possibly be included in the audit
     * @return array an associative array associating the <em>Timestamp</em>
     * of each change to the respective description of the change
     */
    public function audit(){
        return $this->dispatch(func_get_args());
    }

    /**
     * For one or more <em>fields</em>, view the values from all records
     * currently or previously stored.
     *
     * @api
     ** <strong>browse($key)</strong> - Return a view of the values from all
     * records that are currently stored for <em>key</em> and return an
     * ArrayAccess associating each indexed value to an array of records that
     * contain that value in the <em>key</em> field.
     ** <strong>browse($key, $timestamp)</strong> - Return a view of the
     * values from all records that were stored for <em>key</em> at
     * <em>timestamp</em> and return an ArrayAccess associating each indexed
     * value to an array of records that contained that value in the
     * <em>key</em> field at <em>timestamp</em>.
     ** <strong>browse($keys)</strong> - Return a view of the values from all
     * records that are currently stored for each of the <em>keys</em> and
     * return an ArrayAccess associating each of the <em>keys</em> to an
     * ArrayAccess associating each indexed value to an array of records that
     * contain that value in the <em>key</em> field.
     ** <strong>browse($keys, $timestamp)</strong> - Return a view of the
     * values from all records that were stored for each of the <em>keys</em>
     * at timestamp and return an ArrayAccess associating each of the
     * <em>keys</em> to an ArrayAccess associating each indexed value to an
     * array of records that contain that value in the <em>key</em> field at
     * <em>timestamp</em>.
     *
     * @param string $key the field name
     * @param string $keys an array of field names
     * @param integer|string $timestamp the historical timestamp to use in the
     * lookup
     * @return ArrayAccess
     */
    public function browse(){
        return Convert::phpify($this->dispatch(func_get_args()));
    }

    /**
     * Return a time series with snapshots of a <em>field</em> after every
     * change.
     *
     * @api
     ** <strong>chronologize($key, $record)</strong> - Return a time series
     * that contains a snapshot of the values stored for <em>key</em> in
     * <em>record</em> after every change made to the field.
     * the field after the change.
     ** <strong>chronologize($key, $record, $start)</strong> - Return a time
     * series between <em>start</em> (inclusive) and the present that contains
     * a snapshot of the values stored for <em>key</em> in <em>record</em>
     * after every change made to the field during the time span.
     ** <strong>chronologize($key, $record, $start, $end)</strong> - Return a
     * time series between <em>start</em> (inclusive) and <em>end</em>
     * (non-inclusive) that contains a snapshot of the values stored for
     * <em>key</em> in <em>record</em> after every change made to the field
     * during the time span.
     *
     * @param string $key the field name
     * @param integer $record the record id
     * @param integer|string $start the first possible timestamp to include in
     * the time series
     * @param integer|string $end the timestamp that should be greater than
     * every timestamp in the time series
     * @return array associating array associating the timestamp of each
     * change to the list of values that were stored in the field after that
     * change.
     */
    public function chronologize(){
        return Convert::phpify($this->dispatch(func_get_args()));
    }

    /**
     * Atomically remove all the values from one or more fields.
     *
     * @api
     ** <strong>clear($record)</strong> - Atomically remove all the values
     * stored for every key in <em<record</em>.
     ** <strong>clear($records)</strong> - Atomically remove all the values
     * stored for every key in each of the <em>records</em>.
     ** <strong>clear($key, $record)</strong> - Atomically remove all the
     * values stored for <em>key</em> in <em>record</em>.
     ** <strong>clear($keys, $record)</strong> - Atomically remove all the
     * values stored for each of the <em>keys</em> in <em>record</em>.
     ** <strong>clear($key, $records)</strong> - Atomically remove all the
     * values stored for <em>key</em> in each of the <em>records</em>.
     ** <strong>clear($keys, $records)</strong> - Atomically remove all the
     * values stored for each of the <em>keys</em> in each of the .
     * <em>records</em>
     *
     * @param string $key the field name
     * @param array $keys an array of field names
     * @param integer $record the record id
     * @param array $records an array of record ids
     */
    public function clear(){
        $this->dispatch(func_get_args());
    }

    /**
     * Terminate the client's session and close this connection.
     * <em>An alias for the {@link #exit()} method.</em>
     */
    public function close(){
        $this->client->logout($this->creds, $this->environment);
        $this->transport->close();
    }

    /**
     * Attempt to permanently commit any changes that are staged in a
     * transaction and return <em>true</em> if and only if all the changes can
     * be applied. Otherwise, returns <em>false</em> and all the changes are
     * discarded.
     *
     * After returning, the driver will return to <em>autocommit</em> mode and
     * all subsequent changes will be committed immediately.
     *
     * This method will return <em>false</em> if it is called when the driver
     * is not in <em>staging</em> mode.
     *
     */
    public function commit(){
        $token = $this->transaction;
        $this->transaction = null;
        if($token !== null){
            return $this->client->commit($this->creds, $token, $this->environment);
        }
        else {
            return false;
        }
    }

    /**
     * For one or more records list all the keys that have at least one value.
     *
     * @api
     ** <strong>describe($record)</strong> - Return all the keys in
     * <em>record</em> that have at least one value and return that Array of
     * keys.
     ** <strong>describe($record, $timestamp)</strong> - Return all the keys
     * in <em>record</em> that had at least one value at <em>timestamp</em>
     * and return that Array of keys.
     ** <strong>describe($records)</strong> - For each of the <em>records</em>
     * return all of the keys that have at least one value and return an
     * Associative Array associating each of the records to the Array of keys
     * in that record.
     ** <strong>describe($records, $timestamp)</strong> - For each of the
     * <em>records</em>, return all the keys that had at least one value at
     * <em>timestamp</em> and return an Associative Array associating each of
     * the records to the Array of keys that were in that record at
     * <em>timestamp</em>.
     *
     * @param integer $record the record id
     * @param array $records a collection of record ids
     * @param integer|string $timestamp the historical timestamp to use in the
     * lookup
     * @return array
     */
    public function describe(){
        return $this->dispatch(func_get_args());
    }

    /**
     * List the net changes made to a field, record or index from one
     * timestamp to another.
     *
     * If you begin with the state of the <em>record</em> at <em>start</em>
     * and re-apply all the changes in the diff, you'll re-create the state of
     * the <em>record</em> at the present.
     *
     * Unlike the <em>audit(long, Timestamp)</em> method,
     * <em>diff(long, Timestamp) </em> does not necessarily reflect ALL the
     * changes made to <em>record</em> during the time span.
     *
     * @api
     ** <strong>diff($record, $start)</strong> - List the net changes made to
     * <em>record</em> since <em>start</em>. and return an associative array
     * that associates each key in <em>record</em> to another associative
     * array that associates a change description to the array of values that
     * fit the description (i.e. <code>
     * {"key": {ADDED: ["value1", "value2"], REMOVED: ["value3", "value4"]}}
     * </code>).
     ** <strong>diff($record, $start, $end)</strong> - List the net changes
     * made to <em>record</em> from <em>start</em> to <em>end</em> and return
     * an associative array that associates each key in <em>record</em> to
     * another associative array that associates a change description to the
     * array of values that fit the description(i.e. <code>
     * {"key": {ADDED: ["value1", "value2"], REMOVED: ["value3", "value4"]}}
     * </code>).
     ** <strong>diff($key, $record, $start)</strong> - List the net changes
     * made to <em>key</em> in <em>record</em> since <em>start</em> and return
     * an associative array that associates a change description to the array
     * of values that fit the description(i.e. <code>
     * {ADDED: ["value1", "value2"], REMOVED: ["value3", "value4"]}
     * </code>).
     ** <strong>diff($key, $record, $start, $end)</strong> - List the net
     * changes made to <em>key</em> in <em>record</em> from <em>start</em> to
     * <em>end</em> and return an associative array that associates a change description
     * to the array of values that fit the description(i.e. <code>
     * {ADDED: ["value1", "value2"], REMOVED: ["value3", "value4"]}
     * </code>).
     ** <strong>diff($key, $start)</strong> - List the net changes made to the
     * <em>key</em> field across all records since <em>start</em> and return
     * an associative array that associates each value stored for <em>key</em>
     * across all records to another associative array that associates a change
     * description to the array of records where the description applies to
     * that value in the <em>key</em> field(i.e. <code>
     * {"value1": {ADDED: [1, 2], REMOVED: [3, 4]}}
     * </code>).
     ** <strong>diff($key, $start, $end)</strong> - List the net changes made to
     * the <em>key</em> field across all records from <em>start</em> to
     * <em>end</em> and return an associative array that associates each value
     * stored for <em>key</em> across all records to another array that
     * associates a change description to the array of records where the
     * description applies to that value in the <em>key</em> field(i.e. <code>
     * {"value1": {ADDED: [1, 2], REMOVED: [3, 4]}}
     * </code>)
     *
     * @param string $key the field name
     * @param integer $record the record id
     * @param integer|string $start the base timestamp from which the diff is
     * calculated
     * @param integer|string $end the comparison timestamp to which the diff is
     * calculated
     * @return array
     */
    public function diff(){
        return Convert::phpify($this->dispatch(func_get_args()));
    }

    /**
     * Find the records that satisfy the <em>criteria</em>.
     *
     * @api
     *
     ** <strong>find($ccl)</strong> - Return the set of records that satisfy
     * the <em>ccl</em>.
     ** <strong>find($key, $value)</strong> - Return the set of records where
     * <em>key</em> equals <em>value</em>.
     ** <strong>find($key, $value, $timestamp)</strong> - Return the set of
     * records where <em>key</em> equals <em>value</em> at <em>timestamp</em>.
     ** <strong>find($key, $operator, $value)</strong> - Return the set of
     * records where the <em>key</em> field contains at least one value that
     * satisfies <em>operator</em> in relation to <em>value</em>.
     ** <strong>find($key, $operator, $value, $timestamp)</strong> - Return
     * the set of records where the <em>key</em> field contains at least one
     * value that satisfies <em>operator</em> in relation to <em>value</em>
     * at <em>timestamp</em>.
     ** <strong>find($key, $operator, $value, $value2)</strong> - Return the
     * set of records where the <em>key</em> field contains at least one value
     * that satisfies <em>operator</em> in relation to <em>value</em>.
     ** <strong>find($key, $operator, $value, $value2, $timestamp)</strong> -
     * Return the set of records where the <em>key</em> field contains at
     * least one value that satisfies <em>operator</em> in relation to
     * <em>value</em> at <em>timestamp</em>.
     *
     * @param mixed $value the criteria value
     * @param string $key the field/index name
     * @param array $keys the collection of field/index names
     * @param Cinchapi\Concourse\Thrift\Shared\Operator|string $operator The criteria operator
     * @param integer|string $timestamp the timestamp to use when evaluating the criteria
     * @return array the records that match
     */
    public function find(){
        return $this->dispatch(func_get_args());
    }

    /**
     * Return the unique record where the <em>key</em> equals <em>value</em>,
     * or throw a DuplicateEntryException if multiple records match the
     * condition. If no record matches, add <em>key</em> as <em>value</em> in
     * a new record and return the id.
     *
     * This method can be used to simulate a unique index because it atomically
     * checks for a condition and only adds data if the condition isn't
     * currently satisified.
     *
     * @param string $key the field name
     * @param mixed $value the value to find for <em>key</em> or add in a
     * new record
     * @return integer The unique record where <em>key</em> = <em>value</em>,
     * if it exists or the new record where <em>key</em> as <em>value</em> is
     * added.
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
     * Return the unique record that matches the <em>criteria</em>, if one
     * exist or throw a DuplicateEntryException} if multiple records match.
     * If no record matches, <em>insert</em> the <em>data</em> in a new
     * record and return the id.
     *
     * This method can be used to simulate a unique index because it atomically
     * checks for a condition and only inserts data if the condition isn't
     * currently satisified.
     *
     * Each of the values in <em>data</em> must be a primitive or one
     * dimensional object (e.g. no nested <em>associated arrays</em> or <em>multimaps</em>).
     *
     * This method is syntactic sugar for <em>#findOrInsert(Criteria, Map)</em>.
     * The only difference is that this method takes a in-process
     * <em>Criteria</em> building sequence for convenience.
     *
     * @param string $criteria an in-process <em>Criteria</em> building
     * sequence that contains an <em>BuildableState#build()
     * unfinalized</em>, but well-formed filter for the desired
     * record
     * @param mixed $data an <em>associative array</em> with key/value
     * associations to insert into the new record
     * @return integer The unique record that matches the <em>criteria</em>,
     * if it exists or the new record where <em>data</em>
     * is inserted.
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
     ** <strong>get($criteria)</strong> - For every key in every record that
     * matches the <em>criteria</em>, return an associated array associating
     * each of the <em>records</em> to another associated array associating
     * each of the <em>keys</em> to the freshest value in the field.
     ** <strong>get($criteria, $timestamp)</strong> -  For every key in every
     * record that matches the <em>criteria</em>, return an associated array
     * associating each of the <em>records</em> to another associated array
     * associating each of the <em>keys</em> to the freshest value in the
     * field at <em>timestamp</em>.
     ** <strong>get($key, $criteria)</strong> - For every record that matches
     * the <em>criteria</em>, return an <em>associated array</em> associating
     * each of the matching records to the freshest value in the <em>key</em>
     * field.
     ** <strong>get($key, $criteria, $timestamp)</strong> - For every record
     * that matches the <em>criteria</em>, return an <em>associated array</em>
     * associating each of the matching records to the freshest value in the
     * <em>key</em> field at <em>timestamp</em>.
     ** <strong>get($keys, $criteria)</strong> - For each of the <em>keys</em>
     * in every record that matches the <em>criteria</em>, return an associated
     * array associating each of the <em>records</em> to another associated
     * array associating each of the <em>keys</em> to the freshest value in
     * the field.
     ** <strong>get($keys, $criteria, $timestamp)</strong> - For each of the
     * <em>keys</em> in every record that matches the <em>criteria</em>, return
     * an associated array associating each of the <em>records</em> to another
     * associated array associating each of the <em>keys</em> to the freshest
     * value in the field at <em>timestamp</em>.
     ** <strong>get($key, $record)</strong> - Return the stored freshest value
     * in the field that was most recently added for <em>key</em> in
     * <em>record</em>. If the field is empty, return <em>null</em>.
     ** <strong>get($key, $records)</strong> - For each of the <em>records</em>
     * , return an <em>associative array</em> associating each of the
     * <em>records</em> to the freshest value in the <em>key</em> field.
     ** <strong>get($key, $records, $timestamp)</strong> - For each of the
     * <em>records</em>, return an <em>associative array</em> associating each
     * of the <em>records</em> to the freshest value in the <em>key</em> field
     * at <em>timestamp</em>.
     ** <strong>get($key, $record, $timestamp)</strong> - Return the stored
     * freshest value in the field that was most recently added for
     * <em>key</em> in <em>record</em>. If the field is empty, return
     * <em>null</em> at <em>timestamp</em>.
     ** <strong>get($keys, $record)</strong> - For each of the <em>keys</em>
     * in <em>record</em> return an <em>associated array</em> associating each
     * of the <em>keys</em> to the freshest value in the field.
     ** <strong>get($keys, $record, $timestamp)</strong> - For each of the
     * <em>keys</em> in <em>record</em> return an <em>associated array</em>
     * associating each of the <em>keys</em> to the freshest value in the
     * field at <em>timestamp</em>.
     ** <strong>get($keys, $records)</strong> - For each of the <em>keys</em>
     * in each of the <em>records</em>, return a an associated array
     * associating each of the <em>records</em> to another associated array
     * associating each of the <em>keys</em> to the freshest value in the
     * field.
     ** <strong>get($keys, $records, $timestamp)</strong> - For each of the
     * <em>keys</em> in each of the <em>records</em>, return an <em>associated
     * array</em> associating each of the <em>records</em> to another
     * <em>associated array</em> associating each of the <em>keys</em> to the
     * freshest value in the field at <em>timestamp</em>.
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
     * Return the name of the connected environment.
     *
     * @return string the server environment to which this client is connected
     */
    public function getServerEnvironment(){
        return $this->client->getServerEnvironment($this->creds, $this->transaction, $this->environment);
    }

    /**
     * Return the version of the connected server.
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
     ** <strong>insert($data)</strong> - Atomically insert the key/value
     * associations from one or more of the <em>associated arrays</em> in <em>
     * data </em> into new and distinct records and return an <em>array</em>
     * containing the ids of the new records where the maps in <em>data</em>
     * were inserted, respectively and return <em>true</em> if all of the
     * <em>associated array</em> is successfully inserted into <em>record</em>,
     * otherwise <em>false</em>.
     *
     * Each of the values in each map in <em>data<em> must be a primitive or
     * one dimensional object (e.g. no nested <em>associated arrays</em>).
     *
     ** <strong>insert($data, $record)</strong> - Atomically insert the
     * key/value associations from <em>associated array</em> data into
     * <em>record</em>, if possible.
     *
     * The insert will fail if any of the key/value associations in
     * <em>associated array</em> data currently exist in <em>record</em>.
     *
     * Each of the values in <em>associated array</em> must be a primitive or
     * one dimensional object (e.g. no nested <em>associated arrays</em>).
     *
     ** <strong>insert($data, $records)</strong> - Atomically insert the
     * key/value associations from <em>associated arrays</em> data into each of
     * the <em>records</em>, if possible.
     *
     * An insert will fail for a given record if any of the key/value
     * associations in <em>associated arrays</em> data currently exist in that
     * record and return an <em>associated array</em> associating each record
     * id to a boolean that indicates if the <em>data</em> was successfully
     * inserted in that record.
     *
     * Each of the values in <em>data</em> must be a primitive or one
     * dimensional object (e.g. no nested <em>associated arrays</em>).
     *
     ** <strong>insert($json)</strong> - Atomically insert the key/value
     * associations from the {@code json} string into as many new records as
     * necessary and return an <em>array</em> that contains the record ids where
     * the data was inserted.
     *
     * If the <em>json</em> string contains a top-level array (of objects), this
     * method will insert each of the objects in a new and distinct record. The
     * <em>array</em> that is returned will contain the ids of all those
     * records. On the other hand, if the <em>json</em> string contains a
     * single top-level object, this method will insert that object in a single
     * new record. The <em>array</em> that is returned will only contain the id
     * of that record.
     *
     * Regardless of whether the top-level element is an object or an array,
     * each object in the <em>json</em> string contains one or more keys, each
     * of which maps to a JSON primitive or an array of JSON primitives (e.g. no
     * nested objects or arrays).
     *
     ** <strong>insert($json, $records)</strong> - Atomically insert the
     * key/value associations from the <em>json</em> object
     * into each of the <em>records</em>, if possible and return an
     * <em>associative array</em> associating each record id to a boolean that
     * indicates if the <em>json</em> was successfully inserted in that record.
     *
     * An insert will fail for a given record if any of the key/value
     * associations in the <em>json</em> object currently exist in that record.
     *
     * The <em>json</em> must contain a top-level object that contains one or
     * more keys, each of which maps to a JSON primitive or an array of JSON
     * primitives (e.g. no nested objects or arrays).
     *
     ** <strong>insert($json, $record)</strong> - Atomically insert the
     * key/value associations from the <em>json</em> object
     * into <em>record</em>, if possible and return <em>true</em> if the
     * <em>json</em> is inserted into <em>record</em>.
     *
     * The insert will fail if any of the key/value associations in the
     * <em>json</em> object currently exist in <em>record</em>.
     *
     * The <em>json</em> must contain a JSON object that contains one or more
     * keys, each of which maps to a JSON primitive or an array of JSON
     * primitives.
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
     * @return array containing the ids of records that have current or
     *         historical data
     */
    public function inventory(){
        return $this->client->inventory($this->creds, $this->transaction, $this->environment);
    }

    /**
     * Export data as a JSON string.
     *
     * @api
     ** <strong>jsonify($record)</strong> - Atomically dump all the data in
     * <em>record</em> as a JSON object.
     ** <strong>jsonify($record, $timestamp)</strong> - Atomically dump all the
     * data in <em>record<em/> at <em>timestamp</em> as a JSON object.
     ** <strong>jsonify($record, $includeId)</strong> - Atomically dump all the
     * data in <em>record</em> as a JSON object and optionally include a special
     * <em>identifier</em> key that contains the record id.
     ** <strong>jsonify($record, $timestamp, $includeId)</strong> - Atomically
     * dump all the data in <em>record<em> at <em>timestamp</em> as a JSON
     * object and optionally include a special <em>identifier</em> key that
     * contains the record id.
     ** <strong>jsonify($records)</strong> - Atomically dump the data in each
     * of the <em>records</em> as a JSON array of objects.
     ** <strong>jsonify($records, $timestamp)</strong> - Atomically dump the
     * data in each of the <em>records<em> at <em>timestamp<em> as a JSON array
     * of objects.
     ** <strong>jsonify($records, $includeId)</strong> - Atomically dump the
     * data in each of the <em>records</em> as a JSON array of objects and
     * optionally include a special <em>identifier</em> key that contains the
     * record id for each of the dumped objects.
     ** <strong>jsonify($records, $timestamp, $includeId)</strong> - Atomically
     * dump the data in each of the <em>records</em> at <em>timestamp</em> as
     * a JSON array of objects and optionally include a special <em>identifier
     * </em> key that contains the record id for each of the dumped objects.
     *
     * @param integer $record the id of the record to dump
     * @param array $records an array containing the ids of all the records to dump
     * @param integer|string $timestamp the timestamp to use when selecting the data to dump
     * @param boolean $includeId a flag that determines whether record ids are included in the data dump
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
     ** <strong>link($key, $source, $destination)</strong> -Append a link from
     * <em>key</em> in <em>source</em> to <em>destination</em>.
     ** <strong>link($key, $source, $destinations)</strong> - Append links
     * from <em>key</em> in <em>source</em> to each of the
     * <em>destinations</em>
     *
     * @param string $key the field name
     * @param integer $source the source record
     * @param integer $destination the destination record
     * @param array $destinations the destination records
     * @return boolean|array return boolean or an associative array
     * associating the ids for each of the <em>destinations</em> to a boolean
     * that indicates whether the link was successfully added.
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
     ** <strong>ping($record)</strong> - Check to see if <em>record</em>
     * currently contains any data.
     ** <strong>ping($records)</strong> - Atomically check to see if each of
     * the <em>records</em> currently contains any data.
     *
     * @param integer $record the record to ping
     * @param array $records the records to ping
     * @return boolean (<em>true</em> if <em>record</em> currently contains
     * any data, otherwise <em>false</em>) | an associated array associating
     * each of the boolean that indicates whether that record currently
     * contains any data.
     */
    public function ping(){
        return $this->dispatch(func_get_args());
    }

    /**
     * Make the necessary changes to the data stored for <em>key</em> in
     * <em>record</em> so that it contains the exact same <em>values</em> as the
     * specified array.
     *
     * @api
     * <strong>reconcile(key, record, values) - Atomically reconcile the values
     * stored for <em>key</em> in <em>record</em> with those in the specified
     * array.
     *
     * @param string $key the field name
     * @param integer $record the record id
     * @param array $values the array of values that should be exactly what is
     *            contained in the field after this method executes
     */
    public function reconcile(){
        return $this->dispatch(func_get_args());
    }

    /**
     * Remove a value if it exists.
     *
     * @api
     ** <strong>remove($key, $value, $record)</strong> - Remove <em>key</em> as
     * <em>value</em> from <em>record</em> if it currently exists
     * and return <em>true</em> if the data is removed.
     ** <strong>remove($key, $value, $records)</strong> - Atomically remove
     * <em>key</em> as {@code value} from each of the <em>records</em> where
     * it currently exists and return an <em>associated array</em> associating
     * each of the <em>records</em> to a boolean that indicates whether the
     * data was removed.
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
     ** <strong>revert($key, $record, $timestamp)</strong> - Atomically revert
     * <em>key</em> in <em>record</em> to its state at <em>timestamp</em> by
     * creating new revisions that undo the net changes that have occurred
     * since <em>timestamp</em>.
     ** <strong>revert($keys, $record, $timestamp)</strong> - Atomically revert
     * each of the <em>keys</em> in <em>record</em> to their state at
     * </em>timestamp</em> by creating new revisions that undo the net changes
     * that have occurred since <em>timestamp</em>.
     ** <strong>revert($key, $records, $timestamp)</strong> - Atomically revert
     * <em>key</em> in each of the <em>records</em> to its state at
     * <em>timestamp</em> by creating new revisions that undo the net changes
     * that have occurred since <em>timestamp</em>.
     ** <strong>revert($keys, $records, $timestamp)</strong> - Atomically
     * revert each of the <em>keys</em> in each of the <em>records</em> to
     * their state at <em>timestamp</em> by creating new revisions that undo
     * the net changes that have occurred since <em>timestamp</em>
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
     * Perform a full text search for <em>query<em> against the <em>key<em>
     * field and return the records that contain a <em>String<em> or
     * <em>Tag<em> value that matches.
     *
     * @param string $key the field name
     * @param string $query the search query to match
     * @return array array of ids for records that match the search query.
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
     ** <strong>select($key, $record)</strong> - Return all the values stored
     * for <em>key</em> in <em>record</em>. Return an <em>array</em> containing
     * all the values stored in the field.
     ** <strong>select($key, $records)</strong> - Return all values stored for
     * <em>key</em> in each of the <em>records</em>. Return an <em>associated
     * array</em> associating each of the <em>records</em> to an <em>array</em>
     * containing all the values stored in the respective field.
     ** <strong>select($key, $record, $timestamp)</strong> - Return all the
     * values stored for <em>key</em> in <em>record</em> at <em>timestamp</em>.
     * Return an <em>array</em> containing all the values stored in the field
     * at <em>timestamp</em>.
     ** <strong>select($keys, $record)</strong> - Return all the values stored
     * for each of the <em>keys</em> in <em>record</em> and return an
     * <em>associated array</em> associating each of the <em>keys</em> to an
     * <em>array</em> containing all the values stored in the respective field.
     ** <strong>select($keys, $record, $timestamp)</strong> - Return all the
     * values stored for each of the <em>keys</em> in <em>record</em> at
     * <em>timestamp</em>. Return an <em>associated array</em> associating
     * each of the <em>keys</em> to an <em>array</em> containing all the values
     * stored in the respective field at <em>timestamp</em>.
     ** <strong>select($keys, $records, $timestamp)</strong> - return a
     * <em>associated array</em> associating each of the <em>records<em> to
     * another <em>associated array</em> associating each of the <em>keys</em>
     * to an <em>array</em> containing all the values stored in the respective
     * field at <em>timestamp</em>.
     ** <strong>select($keys, $records)</strong> - return a <em>associated
     * array</em> associating each of the <em>records</em> to another
     * <em>associated array</em> associating each of the <em>keys<em> to an
     * <em>array</em> containing all the values stored in the respective field.
     ** <strong>select($key, $records, $timestamp)</strong> - Return all values
     *  stored for <em>key</em> in each of the <em>records</em> at
     * <em>timestamp</em>. Return an <em>associated array</em> associating each
     * of the <em>records</em> to an <em>array</em> containing all the values
     * stored in the respective field at <em>timestamp</em>.
     ** <strong>select($record)</strong> - return an <em>associated array</em>
     * associating each key in <em>record</em> to an <em>array</em> containing
     * all the values stored in the respective field.
     ** <strong>select($record, $timestamp)</strong> - return an <em>associated
     * array</em> associating each key in <em>record</em> to an <em>array</em>
     * containing all the values stored in the respective field at
     * <em>timestamp</em>.
     ** <strong>select($records)</strong> - Return an
     * <em>associated array</em> associating each of the <em>records</em> to
     * another <em>associated array</em> associating every key in that record
     * to an <em>array</em> containing all the values stored in the respective
     * field.
     ** <strong>select($records, $timestamp)</strong> - Return an
     * <em>associated array</em> associating each of the <em>records
     * </em> to another <em>associated array</em> associating every key in
     * that record at <em>timestamp</em> to an <em>array</em> containing all
     * the values stored in the respective field at <em>timestamp</em>.
     *
     * @param string $key a key from which to select values (optional)
     * @param string[] $keys a collection of multiple keys from which to select
     * values (optional)
     * @param integer $record a record from which to select data (required if
     *$criteria and $records is unspecified)
     * @param integer[] $records a collection of multiple keys from which to
     *select data (required if $criteria and $record is unspecified)
     * @param integer|string $timestamp the timestamp to use when selecting data
     *(optional)
     * @return array
     */
    public function select(){
        return Convert::phpify($this->dispatch(func_get_args()));
    }

    /**
     * Atomically remove all existing values from a field and add a new one.
     *
     * @api
     ** <strong>set($key, $value, $record)</strong> - Atomically remove all the
     * values stored for <em>key</em> in <em>record</em> and add then
     * <em>key</em> as <em>value</em>.
     ** <strong>set($key, $value, $records)</strong> - In each of the
     * <em>records</em>, atomically remove all the values stored for
     * <em>key</em> and then add <em>key</em> as <em>value</em> in the
     * respective record.
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
     * <em>staging<em> mode until either #commit or #abort is invoked.
     *
     * All operations that occur within a transaction should be wrapped in a
     * try-catch block so that transaction exceptions can be caught and the
     * transaction can be properly aborted.
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
     * Return the server's unix timestamp in microseconds. The precision of the
     * timestamp is subject to network latency.
     * @api
     ** <strong>time()</strong> - Return a <em>Timestamp</em> that represents
     * the current instant according to the server.
     ** <strong>time($micros)</strong> - Return a <em>Timestamp</em> that
     * corresponds to the specified number of <em>micros</em>econds since the
     * Unix epoch.
     ** <strong>time($phrase)</strong> - Return the <em>Timestamp</em>,
     * according to the server, that corresponds to the instant described by
     * the <em>phrase</em>.
     *
     * @param string $phrase a natural language phrase that describes the desired timestamp (i.e. 3 weeks ago, last month, yesterday at 3:00pm, etc) (optional)
     * @return integer a unix timestamp in microseconds
     */
    public function time(){
        return $this->dispatch(func_get_args());
    }

    /**
     * Remove the link from a key in the <em>source</em> to one or more
     * <em>destination</em> records.
     *
     * @api
     ** <strong>unlink($key, $source, $destination)</strong> - If it exists,
     * remove the link from {@code key} in <em>source</em> to <em>destination
     * </em> and return <em>true</em> if the link is removed.
     *
     * @param string $key the field name
     * @param integer $source the source record
     * @param integer $destination the destination record (required if
     *$destinations is unspecified)
     * @return boolean
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

    /**
     * Remove the link from a key in the <em>source</em> to one or more <em>destination</em> records.
     *
     * @api
     ** <strong>verify($key, $value, $record)</strong> - Return <em>true</em>
     * if <em>value</em> is stored for <em>key</em> in <em>record</em>.
     ** <strong>verify($key, $value, $record, $timestamp)</strong> - Return
     * <em>true</em> if <em>value</em> was stored for <em>key</em> in
     * <em>record</em> at <em>timestamp</em>.
     *
     * @param string $key the field name
     * @param mixed $value the value to check
     * @param integer $record the record id
     * @return boolean
     */
    public function verify(){
        return $this->dispatch(func_get_args());
    }

    /**
     * Atomically replace <em>expected</em> with <em>replacement</em> for
     * <em>key</em> in <em>record</em> if and only if <em>expected</em> is
     * currently stored in the field.
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
     * Atomically verify that <em>key</em> equals <em>expected</em> in
     * <em>record</em> or set it as such.
     *
     * Please note that after returning, this method guarantees that
     * <em>key</em> in <em>record</em> will only contain <em>value</em>, even if
     * it already existed alongside other values (e.g. calling
     * concourse.verifyOrSet("inclusive", "bar", 1) will mean that "inclusive"
     * in record 1 will only have "bar" as a value after returning, even if the
     * field already contained "bar", "baz" and "apple" as values.
     *
     * <em>So, basically, this method has the same guarantee as the [#set]
     * method, except it will not create any new revisions unless it is
     * necessary to do so.</em> The [#set] method, on the other hand, would
     * indiscriminately clear all the values for <em>key</em> in
     * <em>record</em> before adding <em>value</em>, even if <em>value</em>
     * already existed.
     *
     * If you want to add a new value only if it does not exist, while also
     * preserving other values, you should use the [#add] method instead.
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
     * Authenticate the <em>username</em> and <em>password</em> and populate
     * <em>creds</em> with the appropriate AccessToken.
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
