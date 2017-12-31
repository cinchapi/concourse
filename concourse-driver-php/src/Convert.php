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
namespace concourse;

require_once dirname(__FILE__) . "/autoload.php";

use Concourse\Thrift\Shared\Type;
use Concourse\Thrift\Data\TObject;

/**
 * A flag that indicates whether the underlying system uses BIG_ENDIAN byte
 * byte ordering.
 */
define('BIG_ENDIAN', pack('L', 1) === pack('N', 1));

/**
 * The maximum number that is can be represented by a 32 bit integer.
 */
define('MAX_INT', 2147483647);

/**
 * The minimum number that can be represented by a 32 bit integer.
 */
define('MIN_INT', -2147483648);

/**
 * A collection of functions to convert objects to various formats.
 *
 * @author Jeff Nelson
 * @ignore
 */
class Convert {

    /**
     * Convert a PHP object to the appropriate TObject.
     *
     * @param mixed $value the php value to convert
     * @return TObject
     */
    public static function phpToThrift($value) {
        if($value === null) {
            return null;
        }
        else if(is_bool($value)) {
            $type = Type::BOOLEAN;
            $data = pack('c', $value == 1 ? 1 : 0);
        }
        else if (is_int($value)) {
            if ($value > MAX_INT || $value < $MIN_INT) {
                $type = Type::LONG;
                if(!php_supports_64bit_pack()){
                    $data = pack_int64($value);
                    // No need to change the by order here because pack_int64
                    // uses the 'N' format code which is BIG ENDIAN.
                }
                else {
                    $data = pack('q', $value);
                    if (!BIG_ENDIAN) {
                        $data = strrev($data);
                    }
                }
            }
            else {
                $type = Type::INTEGER;
                $data = pack('l', $value);
                if (!BIG_ENDIAN) {
                    $data = strrev($data);
                }
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
        else if(@get_class($value) == "concourse\Tag") {
            $type = Type::TAG;
            $data = utf8_encode(strval($value));
        }
        else if(@get_class($value) == "concourse\Link") {
            $type = Type::LINK;
            if(!php_supports_64bit_pack()){
                $data = pack_int64($value->getRecord());
                // No need to change the by order here because pack_int64
                // uses the 'N' format code which is BIG ENDIAN.
            }
            else {
                $data = pack('q', $value->getRecord());
                if (!BIG_ENDIAN) {
                    $data = strrev($data);
                }
            }
        }
        else if(@get_class($value) == "DateTime") {
            $type = Type::TIMESTAMP;
            $micros = $value->getTimestamp() * 1000000;
            if(!php_supports_64bit_pack()){
                $data = pack_int64($micros);
                // No need to change the by order here because pack_int64
                // uses the 'N' format code which is BIG ENDIAN.
            }
            else {
                $data = pack('q', $micros);
                if (!BIG_ENDIAN) {
                    $data = strrev($data);
                }
            }
        }
        else {
            $type = Type::STRING;
            $data = utf8_encode(strval($value));
        }
        return new TObject(array('type' => $type, 'data' => $data));
    }

    /**
     * Convert a TObject to the correct PHP object.
     *
     * @param TObject $tobject the TObject to convert
     * @return mixed
     */
    public static function thriftToPhp(TObject $tobject) {
        $php = null;
        $data = $tobject->data;
        switch ($tobject->type) {
            case Type::BOOLEAN:
                $php = unpack('c', $tobject->data)[1];
                $php = $php == 1 ? true : false;
                break;
            case Type::INTEGER:
                $data = !BIG_ENDIAN ? strrev($data) : $data;
                $php = unpack('l', $data)[1];
                break;
            case Type::LONG:
                if(!php_supports_64bit_pack()){
                    // No need to change the by order here because unpack_int64
                    // uses the 'N' format code which is BIG ENDIAN.
                    $php = unpack_int64($data);
                }
                else {
                    $data = !BIG_ENDIAN ? strrev($data) : $data;
                    $php = unpack('q', $data)[1];
                }
                break;
            case Type::DOUBLE:
            case Type::FLOAT:
                $data = !BIG_ENDIAN ? strrev($data) : $data;
                $php = unpack('d', $data)[1];
                break;
            case Type::TAG:
                $php = utf8_decode($tobject->data);
                $php = Tag::create($php);
                break;
            case Type::LINK:
                if(!php_supports_64bit_pack()){
                    // No need to change the by order here because unpack_int64
                    // uses the 'N' format code which is BIG ENDIAN.
                    $php = unpack_int64($data);
                }
                else {
                    $data = !BIG_ENDIAN ? strrev($data) : $data;
                    $php = unpack('q', $data)[1];
                }
                $php = Link::to($php);
                break;
            case Type::TIMESTAMP:
                if(!php_supports_64bit_pack()){
                    // No need to change the by order here because unpack_int64
                    // uses the 'N' format code which is BIG ENDIAN.
                    $php = unpack_int64($data);
                }
                else {
                    $data = !BIG_ENDIAN ? strrev($data) : $data;
                    $php = unpack('q', $data)[1];
                }
                $dt = new \DateTime();
                $php = $dt->setTimestamp($php / 1000000);
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
     *
     * @param mixed $data a collection that contains TObjects
     * @return mixed a purely PHP data structure
     */
    public static function phpify($data) {
        if(is_assoc_array($data)) {
            $new = [];
            foreach($data as $k => $v) {
                $k = try_unserialize($k, $result) ? $result : $k;
                $k = static::isTObject($k) ? static::thriftToPhp($k) : static::phpify($k);
                if(!is_integer($k) && !is_string($k) && !is_object($new)) {
                    //PHP arrays can only contain string|integer keys, so in the
                    // event that we have something else, we must use a class
                    // that implements the ArrayAccess interface
                    $temp = new Dictionary();
                    foreach($new as $nk => $nv) {
                        $temp[$nk] = $nv;
                    }
                    $new = $temp;
                }
                $v = static::isTObject($v) ? static::thriftToPhp($v) : static::phpify($v);
                $new[$k] = $v;
            }
            return $new;
        }
        else if(is_array($data)) {
            $newData = [];
            foreach($data as $item) {
                $newData[] = static::phpify($item);
            }
            return $newData;
        }
        else if(static::isTObject($data)) {
            return static::thriftToPhp($data);
        }
        else{
            return $data;
        }
    }

    /**
     * Recurisvely convert any nested PHP objects to Thrift compatible objects.
     *
     * @param mixed $data a collection of PHP objects
     * @return mixed TObject or collection of TObject
     */
    public static function thriftify($data) {
        if(is_assoc_array($data)) {
            foreach($data as $k => $v) {
                unset($data[$k]);
                $k = !is_array($k) ? static::phpToThrift($k) : static::thriftify($k);
                $k = !is_array($k) ? static::phpToThrift($v) : static::thriftify($v);
                $data[$k] = $v;
            }
            return $data;
        }
        else if(is_array($data)) {
            $newData = [];
            foreach($data as $item) {
                $newData[] = static::thriftify($item);
            }
            return $newData;
        }
        else if(static::isTObject($var)) {
            return static::phpToThrift($var);
        }
        else{
            return $var;
        }
    }

    /**
     * Return {@code true} if {@code $var is a TObject}.
     *
     * @param mized $var
     * @return bool
     */
    private static function isTObject($var) {
        return is_object($var) && str_ends_with(get_class($var), "TObject");
    }

}
