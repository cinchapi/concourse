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

$kwarg_aliases = array(
    'criteria' => function($kwargs){
        return $kwargs["ccl"] ?: $kwargs["where"] ?: $kwargs["query"];
    },
    'timestamp' => function($kwargs){
        return $kwargs["time"] ?: $kwargs["ts"];
    },
    'username' => function($kwargs){
        return $kwargs["user"] ?: $kwargs["uname"];
    },
    'password' => function($kwargs){
        return $kwargs["pass"] ?: $kwargs["pword"];
    },
    'prefs' => function($kwargs){
        return $kwargs["file"] ?: $kwargs["filename"] ?: $kwargs["config"] ?: $kwargs["path"];
    },
    'expected' => function($kwargs){
        return $kwargs["value"] ?: $kwargs["current"] ?: $kwargs["old"];
    },
    'replacement' => function($kwargs){
        return $kwargs["new"] ?: $kwargs["other"] ?: $kwargs["value2"];
    }
            
);

function find_in_kwargs_by_alias($key, $kwargs){
    global $kwarg_aliases;
    return $kwargs[$key] ?: $kwarg_aliases[$key]($kwargs);
}

