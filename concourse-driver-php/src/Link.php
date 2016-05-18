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
 * A Link is a pointer to a record.
 *
 * <p>
 * Link should never be written directly. They can be created using the
 * <em>#link()</em> methods in the Concourse API.
 * </p>
 * <p>
 * Links may be returned when reading data using the <em>#select()</em>,
 * <em>#get()</em> and <em>#browse()</em> methods. When handling Link
 * objects, you can retrieve the underlying record id by calling
 * <em>Link#getRecord()</em>.
 * </p>
 * <p>
 * When performing a bulk insert (using the <em>Concourse#insert()</em> method),
 * you can use this class to create Link objects that are added to the data/json
 * blob. Links inserted in this manner will be written in the same way they
 * would have been if they were written using the <em>Concourse#link()</em> API
 * method.
 * </p>
 * <p>
 * To create a static link to a single record, use <em>Link.to(record)</em>.
 * </p>
 * <p>
 * To create static links to each of the records that match a criteria, use the
 * <em>Link.toWhere()</em> methods.
 * </p>
 *
 * @author Jeff Nelson
 */
class Link implements \JsonSerializable {

    /**
     * Return a <em>Link</em> that points to <em>record</em>.
     *
     * @param  integer $record the record id
     * @return Link a <em>Link</em> that poins to <em>record</em>
     */
    public static function to($record) {
        return new Link($record);
    }

    /**
     * Return a string that instructs Concourse to create links that point to
     * each of the records that match the {@code ccl} string.
     *
     * <p>
     * <strong>NOTE:</strong> This method DOES NOT return a <em>Link</em>
     * object, so it should only be used when adding a <em>resolvable link</em>
     * value to a data/json blob that will be passed to the <em>Concourse.insert()</em> method.
     * </p>
     *
     * @param  String $ccl a CCL string that describes the records to which a
     *                     Link should point
     * @return String a resolvable link instruction
     */
    public static function toWhere($ccl) {
        return "@$ccl@";
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
