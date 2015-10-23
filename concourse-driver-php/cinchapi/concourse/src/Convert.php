<?php
require_once dirname(__FILE__) . "/autoload.php";

use Cinchapi\Concourse\Core as core;
use Thrift\Shared\Type;
use Thrift\Data\TObject;

define('BIG_ENDIAN', pack('L', 1) === pack('N', 1));
define('MAX_INT', 2147483647);
define('MIN_INT', -2147483648);

/**
 * @ignore
 */
class Convert {

    /**
     * Convert a PHP object to the appropriate TObject.
     * @param mixed $value
     * @return TObject
     */
    public static function phpToThrift($value) {
        if(is_null($value)){
            return null;
        }
        else if(is_bool($value)){
            $type = Type::BOOLEAN;
            $data = pack('c', $value == 1 ? 1 : 0);
        }
        else if (is_int($value)) {
            if ($value > MAX_INT || $value < $MIN_INT) {
                $type = Type::LONG;
                $data = core\php_supports_64bit_pack() ? pack('q', $value)
                        : core\pack_int64($value);
            }
            else {
                $type = Type::INTEGER;
                $data = pack('l', $value);
            }
            if (!BIG_ENDIAN) {
                $data = strrev($data);
            }
        }
        else if (is_float($value)) {
            $type = Type::DOUBLE;
            $data = pack('d', $value);
            if (!BIG_ENDIAN) {
                $data = strrev($data);
            }
            //TODO what about double?
        }
        else if(@get_class($value) == "Tag"){
            $type = Type::TAG;
            $data = utf8_encode(strval($value));
        }
        else if(@get_class($value) == "Link"){
            $type = Type::LINK;
            $data = core\php_supports_64bit_pack() ? pack('q', $value->getRecord())
                        : core\pack_int64($value->getRecord());
            if (!BIG_ENDIAN) {
                $data = strrev($data);
            }
        }
        else {
            $type = Type::STRING;
            $data = utf8_encode(strval($value));
        }
        return new TObject(array('type' => $type, 'data' => $data));
    }

    public static function stringToTime($time){
        return strtotime($time) * 1000;
    }

    /**
     * Convert a TObject to the correct PHP object.
     * @param TObject $tobject
     * @return mixed
     */
    public static function thriftToPhp(TObject $tobject){
        $php = null;
        switch ($tobject->type){
            case Type::BOOLEAN:
                $php = unpack('c', $tobject->data)[1];
                $php = $php == 1 ? true : false;
                break;
            case Type::INTEGER:
                $data = !BIG_ENDIAN ? strrev($tobject->data) : $data;
                $php = unpack('l', $data)[1];
                break;
            case Type::LONG:
                $data = !BIG_ENDIAN ? strrev($tobject->data) : $data;
                $php = core\php_supports_64bit_pack() ? unpack('q', $data)[1]
                        : core\unpack_int64($data);
                break;
            case Type::DOUBLE:
            case Type::FLOAT:
                $data = !BIG_ENDIAN ? strrev($tobject->data) : $data;
                $php = unpack('d', $data)[1];
                break;
            case Type::TAG:
                $php = utf8_decode($tobject->data);
                $php = Tag::create($php);
                break;
            case Type::LINK:
                $data = !BIG_ENDIAN ? strrev($tobject->data) : $data;
                $php = core\php_supports_64bit_pack() ? unpack('q', $data)
                        : core\unpack_int64($data);
                $php = Link::to($php);
                break;
            case Type::STRING:
                $php = utf8_decode($tobject->data);
                break;
            default:
                break;
        }
        return $php;
    }

    /**
     * Recurisvely convert any nested TObjects to PHP objects.
     * @param mixed $data
     * @return a purely PHP data structure
     */
    public static function phpify($data){
        if(is_assoc_array($data)){
            $new = [];
            foreach($data as $k => $v){
                $k = try_unserialize($k, $result) ? $result : $k;
                $k = static::isTObject($k) ? static::thriftToPhp($k) : static::phpify($k);
                if(!is_integer($k) && !is_string($k) && !is_object($new)){
                    //PHP arrays can only contain string|integer keys, so in the
                    // event that we have something else, we must use a class
                    // that implements the ArrayAccess interface
                    $temp = new Dictionary();
                    foreach($new as $nk => $nv){
                        $temp[$nk] = $nv;
                    }
                    $new = $temp;
                }
                $v = static::isTObject($v) ? static::thriftToPhp($v) : static::phpify($v);
                $new[$k] = $v;
            }
            return $new;
        }
        else if(is_array($data)){
            $newData = [];
            foreach($data as $item){
                $newData[] = static::phpify($item);
            }
            return $newData;
        }
        else if(static::isTObject($data)){
            return static::thriftToPhp($data);
        }
        else{
            return $data;
        }
    }

    /**
     * Recurisvely convert any nested PHP objects to Thrift compatible objects.
     * @param mixed $data
     * @return a TObject or collection of TObject
     */
    public static function thriftify($data){
        if(is_assoc_array($data)){
            foreach($data as $k => $v){
                unset($data[$k]);
                $k = !is_array($k) ? static::phpToThrift($k) : static::thriftify($k);
                $k = !is_array($k) ? static::phpToThrift($v) : static::thriftify($v);
                $data[$k] = $v;
            }
            return $data;
        }
        else if(is_array($data)){
            $newData = [];
            foreach($data as $item){
                $newData[] = static::thriftify($item);
            }
            return $newData;
        }
        else if(static::isTObject($var)){
            return static::phpToThrift($var);
        }
        else{
            return $var;
        }
    }

    /**
     * Return {@code true} if {@code $var is a TObject}.
     * @param mized $var
     * @return bool
     */
    private static function isTObject($var){
        return is_object($var) && str_ends_with(get_class($var), "TObject");
    }

}
