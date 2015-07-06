<?php

/* 
 * Copyright 2015 Cinchapi, Inc.
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
        $aliases = $kwarg_aliases[$key];
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

