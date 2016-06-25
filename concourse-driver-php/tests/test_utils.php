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
 * Generate and return a random string with {@code $length} characters selected
 * from {@code valid_chars).
 * @param int $length
 * @param string $valid_chars
 * @return string
 */
function random_string($length = 8, $valid_chars = "abcdefghijklmnopqrstuvwxyz0123456789"){
    $string = "";
    if(is_array($valid_chars)){
        $valid_chars = implode($valid_chars);
    }
    $num_valid_chars = strlen($valid_chars);
    for($i = 0; $i < $length; ++$i){
        $index = rand(0, $num_valid_chars);
        $char = $valid_chars[$index];
        $char = mt_rand(0, 10) % 2 == 0 ? strtoupper($char) : $char;
        $string.= $char;
    }
    return $string;
}

function scale_count(){
    return rand(0, 90) + 10;
}

function random_int(){
    return rand(MIN_INT, MAX_INT);
}

function random_long(){
    return rand(PHP_INT_MIN, PHP_INT_MAX);
}

function random_bool() {
    return random_int() % 2 == 0 ? true: false;
}
