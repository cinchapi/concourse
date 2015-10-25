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

#########################################################################
# This file contains core library functions that are used throughout    #
# the project for simplicity.                                           #
#########################################################################
namespace concourse;

/**
 * @ignore
 * Throw an IllegalArgumentException that explains that an arg is required.
 * @param type $arg
 * @throws InvalidArgumentException
 */
function require_arg($arg){
    $caller = debug_backtrace()[1]['function']."()";
    throw new \InvalidArgumentException($caller." requires the ".$arg." positional "
    . "or keyword argument(s).");
}

/**
 * @ignore
 * The kwarg_aliases.
 */
$kwarg_aliases = array(
    'ccl' => array("criteria", "where", "query"),
    'time' => array("timestamp", "ts"),
    'username' => array("user", "uname"),
    'password' => array("pass", "pword"),
    'prefs' => array("file", "filename", "config", "path"),
    'expected' => array("value", "current", "old"),
    'replacement' => array("new", "other", "value2"),
    'json' => array('data')
);

/**
 * @ignore
 * Find a value for a key in the given {@code $kwargs} by the key itself or one
 * of the aliases defined in {@code $kwarg_aliases}.
 * @global array $kwarg_aliases
 * @param type $key
 * @param type $kwargs
 * @return mixed
 */
function find_in_kwargs_by_alias($key, $kwargs){
    global $kwarg_aliases;
    $value = $kwargs[$key];
    if(empty($value)){
        $aliases = $kwarg_aliases[$key] ?: [];
        foreach($aliases as $alias){
            $value = $kwargs[$alias];
            if(!empty($value)){
                break;
            }
        }
    }
    return $value;
}

/**
 * @ignore
 * Given arguments to a function (retrieved using func_get_args()), return an array listing an array of the positional arguments first, followed by an array of the keyword arguments.
 * @param  array $func_args retrieved using func_get_args()
 * @return array [$args, $kwargs]
 */
function gather_args_and_kwargs($func_args){
    $args = $func_args;
    $kwargs = [];
    foreach($args as $index => $arg){
        if(is_assoc_array($arg)){
            $kwargs = $arg;
            unset($args[$index]);
        }
    }
    return [$args, $kwargs];
}

/**
 * @ignore
 * A hack to pack a 64 bit int in PHP versions that don't support this natively.
 * @param int $value
 * @return binary
 */
function pack_int64($value){
    $highMap = 0xffffffff00000000;
    $lowMap = 0x00000000ffffffff;
    $higher = ($value & $highMap) >> 32;
    $lower = $value & $lowMap;
    $packed = pack('L2', $higher, $lower);
    return $packed;
}

/**
 * @ignore
 * A hack to unpack a 64 bit int in PHP versions that don't support this
 * natively.
 * @param int $packed
 * @return int
*/
function unpack_int64($packed){
    list($higher, $lower) = array_values(unpack('L2', $packed));
    $value = $higher << 32 | $lower;
    return $value;
}

/**
 * @ignore
 * Return {@code true} if the PHP version supports 64bit pack/unpack format codes.
 * @return boolean
 */
function php_supports_64bit_pack(){
    return version_compare(PHP_VERSION, "5.6.3") >= 0;
}
