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
 # This file is the central place to require everything that needs to be #
 # loaded throughout the project.                                        #
 #########################################################################
$vendor_autoload = dirname(__FILE__) . "/../vendor/autoload.php";
if(file_exists($vendor_autoload)){
    // If the package is running locally, then we must manually handle vendor
    // autoloading that composer would automatically take care of.
    require_once $vendor_autoload;
}
require_directory(dirname(__FILE__));

/**
 * @ignore
 * Require all of the files in the specified $directory and recursively  do so
 * if $recursive is TRUE.
 *
 * In general, it is probably more efficient to use an autoloader, however this
 * function is provided for directories that contain files for which most are
 * known to be required when the script starts.
 *
 * @param string $directory The directory that contains the files to require
 * @param string $recursive A flag that controls whether this function will
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

/**
 * @ignore
 * Return TRUE if the $haystack ends with the $needle.
 *
 * @param string $haystack The string to search
 * @param string $needle The desired prefix
 * @return boolean TRUE if $haystack starts with $needle
 */
function str_ends_with($haystack, $needle){
    return $needle === "" || (($temp = strlen($haystack) - strlen($needle)) >= 0 && strpos($haystack, $needle, $temp) !== FALSE);
}
