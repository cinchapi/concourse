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
# This file contains functions that should be in the PHP core, but are  #                                    # not. These functions are not namespaced.
#########################################################################

/**
 * Returns the current time in milliseconds subject to the granularity of the
 * underlying operating system.
 *
 * @return integer
 */
function current_time_millis(){
    return round(microtime(true) * 1000);
}

/**
 * Prints a message and then terminates the line.
 *
 * @param mixed $message the primitive, array or object to print
 */
function println($message){
    print_r($message);
    if(!empty($_SERVER['HTTP_USER_AGENT'])){
        // Assume this is being displayed on a web page
        print_r("<br />");
    }
    else {
        print_r(PHP_EOL);
    }
}
