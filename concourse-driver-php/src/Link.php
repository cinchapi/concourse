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
 * A Link is a wrapper around a {@link Long} that represents the primary key of
 * a record and distinguishes from simple long values. A Link is returned from
 * read methods in Concourse if data was added using one of the #link operations.
 */
class Link{

    public static function to($record){
        return new Link($record);
    }

    private $record;

    private function __construct($record) {
        $this->record = $record;
    }

    public function __toString() {
        return "@".$this->record."@";
    }

    public function getRecord(){
        return $this->record;
    }

}
