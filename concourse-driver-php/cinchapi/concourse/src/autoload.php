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
 * This file is the central place to require everything that needs to be
 * autoloaded throughout the project.
 */
require_once dirname(__FILE__) . "/../../../vendor/autoload.php";
require_directory(dirname(__FILE__));

/**
* @ignore
* Return {@code true} if the {@code $haystack} ends with the {@code $needle}.
* @param string $haystack
* @param string $needle
* @return boolean
*/
function str_ends_with($haystack, $needle){
    return $needle === "" || (($temp = strlen($haystack) - strlen($needle)) >= 0 && strpos($haystack, $needle, $temp) !== FALSE);
}

/**
 * @ignore
 * Require all of the files in the specified {@code $directory} and recursively * do so if {@code $recursive} is true.
 * @param string $directory - the directory that contains the files to require
 * @param string $recursive - a flag that controls whether this function will
 * descend into subdirectories to require those files (default: true).
 */
function require_directory($directory, $recursive=true){
    foreach(scandir($directory) as $filename){
        $path = $directory . DIRECTORY_SEPARATOR . $filename;
        if($recursive && is_dir($path) && !str_ends_with($path, ".")){
            require_directory($path, $recursive);
        }
        else if (str_ends_with($path, ".php")){
            require_once $path;
        }
    }
}
