<?php
require_once dirname(__FILE__)."/../../../vendor/autoload.php";
require_once dirname(__FILE__)."/thrift/ConcourseService.php";
require_once dirname(__FILE__)."/thrift/shared/Types.php";
require_once dirname(__FILE__)."/thrift/data/Types.php";
require_once dirname(__FILE__)."/Convert.php";

use Thrift\Transport\TSocket;
use Thrift\Transport\TBufferedTransport;
use Thrift\Protocol\TBinaryProtocolAccelerated;
use Thrift\ConcourseServiceClient;
use Thrift\Data\TObject;
use Thrift\Shared\TransactionToken;
use Thrift\Shared\Type;
use Thrift\Shared\AccessToken;

class Concourse {

  public static function connect($host="localhost", $port=1717,
    $username="admin", $password="admin", $environment=""){
      return new Concourse($host, $port, $username, $password, $environment);
    }

  private $host;
  private $port;
  private $username;
  private $password;
  private $environment;
  private $client;
  private $transaction;

  public function __construct($host, $port, $username, $password, $environment){
      $this->host = $host;
      $this->port = $port;
      $this->username = $username;
      $this->password = $password;
      $this->environment = $environment;
      try {
        $socket = new TSocket($host, $port);
        $transport = new TBufferedTransport($socket);
        $protocol = new TBinaryProtocolAccelerated($transport);
        $this->client = new ConcourseServiceClient($protocol);
        $transport->open();
        $this->authenticate();
      }
      catch(TException $e){
        throw new Exception("Could not connect to the Concourse Server at ".$host.":".$port);
      }
    }

  private function authenticate(){
    try{
      $this->creds = $this->client->login($this->username, $this->password, $this->environment);
    }
    catch(TException $e){
      throw e;
    }
  }

  public function add($key, $value, $records=null){
    $value = Convert::phpToThrift($value);
    if(empty($records)){
      return $this->client->addKeyValue($key, $value, $this->creds, $this->transaction, $this->environment);
    }
    else{

    }
  }
}
