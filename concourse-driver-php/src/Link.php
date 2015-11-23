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
 * A Link is a wrapper to distinguish a record's primary key from other
 * integers. Links are returned for reads whenever data was written if
 *  using one of the #link operations.
 *
 * @author Jeff Nelson
 */
class Link implements \JsonSerializable {

    /**
     * Create a new <em>Link</em> that points to the record identified by the
     * primary key <em>id</em>.
     *
     * Users should not create Links directly. Use the <em>#link</em> methods in
     * the <em>Concourse</em> client to write links to the database.
     *
     * @param  integer $id the primary key of the record to which the Link points
     * @return Link the Link that wraps the primary key
     */
    public static function to($id) {
        return new Link($id);
    }

    /**
     * The underlying record id to which the Link points.
     * @var integer
     */
    private $record;

    /**
     * Construct a new instance.
     *
     * @param  integer $record the primary key of the record to which the Link
     */
    private function __construct($record) {
        $this->record = $record;
    }

    /**
     * Return the record id to which this Link points.
     *
     * @return integer the underlying record id
     */
    public function getRecord(){
        return $this->record;
    }

    /**
     * @Override
     */
    public function jsonSerialize() {
        return $this->__toString();
    }

    /**
     * Return a string representation of this object.
     *
     * @return string the object encoded as a string
     */
    public function __toString() {
        return "@".$this->record;
    }

}
