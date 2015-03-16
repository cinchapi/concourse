<?php
require_once dirname(__FILE__)."/../../../vendor/autoload.php";
require_once dirname(__FILE__)."/thrift/ConcourseService.php";
require_once dirname(__FILE__)."/thrift/shared/Types.php";
require_once dirname(__FILE__)."/thrift/data/Types.php";

use Thrift\Shared\Type;
use Thrift\Data\TObject;

define('BIG_ENDIAN', pack('L', 1) === pack('N', 1));
define('MAX_INT', 2147483647);
define('MIN_INT', -2147483648);

class Convert {

  public static function phpToThrift($value){
    if(is_int($value)){
      if($value > MAX_INT || $value < $MIN_INT){
        $type = Type::LONG;
        $data = pack('q', $value);
      }
      else{
        $type = Type::INTEGER;
        $data = pack('l', $value);
      }
      if(!BIG_ENDIAN){
        $data = strrev($data);
      }
    }
    else if(is_float($value)){
      $type = Type::FLOAT;
      $data = pack('f', $value);
      if(!BIG_ENDIAN){
        $data = strrev($data);
      }
    }
    else{
      $type = Type::STRING;
      $data = utf8_encode(strval($value));
    }
    return new TObject(array('type' => $type, 'data' => $data));
  }
}
