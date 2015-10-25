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

/**
 * A Tag is a String data type that does not get full-text indexed.
 *
 * Each Tag is equivalent to its String counterpart. Tags merely exist for the
 * client to instruct Concourse not to full text index the data. Tags are stored
 * as Strings within Concourse. Any data value that is written as a Tag is always
 * returned as a String when reading from Concourse.
 */
class Tag {

    private $value;

    public static function create($value){
        return new Tag($value);
    }

    private function __construct($value){
        $this->value = $value;
    }

    public function __toString(){
        return $this->value;
    }


}
