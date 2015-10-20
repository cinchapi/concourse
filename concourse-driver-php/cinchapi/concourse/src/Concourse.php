<?php
require_once dirname(__FILE__) . "/autoload.php";

use Cinchapi\Concourse\Core as core;
use Thrift\Transport\TSocket;
use Thrift\Transport\TBufferedTransport;
use Thrift\Protocol\TBinaryProtocolAccelerated;
use Thrift\ConcourseServiceClient;
use Thrift\Data\TObject;
use Thrift\Shared\TransactionToken;
use Thrift\Shared\Type;
use Thrift\Shared\AccessToken;
use Thrift\Exceptions\TransactionException;

/**
* Concourse is a self-tuning database that makes it easier for developers to
* quickly build robust and scalable systems. Concourse dynamically adapts on a
* per-application basis and offers features like automatic indexing, version
* control, and distributed ACID transactions within a big data platform that
* reduces operational complexity. Concourse abstracts away the management and
* tuning aspects of the database and allows developers to focus on what really
* matters.
*/
class Concourse {

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
            $prefs = core\find_in_kwargs_by_alias("prefs", $kwargs);
            if(!empty($prefs)){
                $prefs = parse_ini_file(core\expand_path($prefs));
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
        $this->username = $prefs["username"] ?: core\find_in_kwargs_by_alias('username', $kwargs) ?: $username;
        $this->password = $prefs["password"] ?: core\find_in_kwargs_by_alias("password", $kwargs) ?: $password;
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

    public function insert(){
        return $this->dispatch(func_get_args());
    }

    public function logout(){
        $this->client->logout($this->creds, $this->environment);
    }

    public function remove(){
        return $this->dispatch(func_get_args());
    }

    public function select(){
        return Convert::phpify($this->dispatch(func_get_args()));
    }

    public function set(){
        $this->dispatch(func_get_args());
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

    public function time(){
        return $this->dispatch(func_get_args());
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
        $tocall = Dispatcher::send($method, $args, $kwargs);
        $callback = array($this->client, array_keys($tocall)[0]);
        $params = array_values($tocall)[0];
        $params[] = $this->creds;
        $params[] = $this->transaction;
        $params[] = $this->environment;
        return call_user_func_array($callback, $params);
    }

}
