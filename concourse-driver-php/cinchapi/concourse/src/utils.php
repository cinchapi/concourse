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

/**
* @ignore
* Throw an IllegalArgumentException that explains that an arg is required.
* @param type $arg
* @throws InvalidArgumentException
*/
function require_arg($arg){
    $caller = debug_backtrace()[1]['function']."()";
    throw new InvalidArgumentException($caller." requires the ".$arg." positional "
    . "or keyword argument(s).");
}

/**
* The kwarg_aliases.
*/
$kwarg_aliases = array(
    'criteria' => array("ccl", "where", "query"),
    'timestamp' => array("time", "ts"),
    'username' => array("user", "uname"),
    'password' => array("pass", "pword"),
    'prefs' => array("file", "filename", "config", "path"),
    'expected' => array("value", "current", "old"),
    'replacement' => array("new", "other", "value2"),
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
* Expand any tilde's and ".." components of the path.
* @param string $path
* @return the real path
*/
function expand_path($path){
    if (function_exists('posix_getuid') && strpos($path, '~') !== false) {
        $info = posix_getpwuid(posix_getuid());
        $path = str_replace('~', $info['dir'], $path);
    }
    $newpath = realpath($path);
    return $newpath ?: $path;
}

/**
* @ignore
* Return {@code true} if {@code $var} is an assoc array.
* @param mixed $var
* @return boolean
*/
function is_assoc_array($var){
    if(is_array($var)) {
        // http://stackoverflow.com/a/4254008
        return (bool)count(array_filter(array_keys($var), 'is_string'));
    }
    else {
        return false;
    }
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

/**
* @ignore
* Return {@code true} if the {@code $haystack} begins with the {@code $needle}.
* @param string $haystack
* @param string $needle
* @return boolean
*/
function string_starts_with($haystack, $needle){
    $length = strlen($needle);
    return (substr($haystack, 0, $length) === $needle);
}

/**
* @ignore
* Return {@code true} if the {@code $haystack} ends with the {@code $needle}.
* @param string $haystack
* @param string $needle
* @return boolean
*/
function string_ends_with($haystack, $needle){
    $length = strlen($needle);
    $start  = $length * -1;
    return (substr($haystack, $start) === $needle);
}

/**
* @ignore
* Return {@code true} if the {@code $haystack} contains the {@code $needle}.
* @param string $haystack
* @param string $needle
* @return boolean
*/
function string_contains($haystack, $needle){
    return strpos($haystack, $needle) !== false;
}
