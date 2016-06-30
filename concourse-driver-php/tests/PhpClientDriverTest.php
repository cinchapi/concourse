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
 require_once dirname(__FILE__) . "/IntegrationBaseTest.php";

use Concourse\Thrift\Shared\Type;
use Concourse\Thrift\Shared\Diff;
use Concourse\Thrift\Shared\Operator;
use Concourse\Convert;
use Concourse\Tag;
use Concourse\Link;
use Concourse\Thrift\Complex\ComplexTObject;

/**
 * Description of PhpClientDriverTest
 *
 * @author jnelson
 */
 class PhpClientDriverTest extends IntegrationBaseTest {

    private function doTestValueRoundTrip($value, $type){
        $key = random_string();
        $record = $this->client->add($key, $value);
        $stored = $this->client->get(['key' => $key, 'record' => $record]);
        $this->assertEquals($value, $stored);
        $this->assertEquals(Convert::phpToThrift($stored)->type, $type);
    }

    public function testStringRoundTrip(){
        $this->doTestValueRoundTrip(random_string(), Type::STRING);
    }

    public function testBooleanRoundTrip(){
        $this->doTestValueRoundTrip(rand(0, 10 ) % 2 == 0 ? true : false, Type::BOOLEAN);
    }

    public function testTagRoundTrip(){
        $this->doTestValueRoundTrip(Tag::create(random_string()), Type::TAG);
    }

    public function testLinkRoundTrip(){
        $this->doTestValueRoundTrip(Link::to(rand(0, PHP_INT_MAX)), Type::LINK);
    }

    public function testIntRoundTrip(){
        $this->doTestValueRoundTrip(rand(MIN_INT, MAX_INT), Type::INTEGER);
    }

    public function testLongRoundTrip(){
        $this->doTestValueRoundTrip(rand(MAX_INT+1, PHP_INT_MAX), Type::LONG);
    }

    public function testDoubleRoundTrip(){
        $this->doTestValueRoundTrip(3.4028235E38, Type::DOUBLE);
        $this->doTestValueRoundTrip(-1.4E-45, Type::DOUBLE);
    }

    public function testAbort(){
        $this->client->stage();
        $key = random_string();
        $value = "some value";
        $record = 1;
        $this->client->add(['key' => $key, 'value' => $value, 'record' => $record]);
        $this->client->abort();
        $this->assertNull($this->client->get(['key' => $key, 'record' => $record]));
    }

    public function testAddKeyValue(){
        $key = "foo";
        $value = "static value";
        $record = $this->client->add(['key' => $key, 'value' => $value]);
        $this->assertNotEmpty($record);
        $stored = $this->client->get(['key' => $key, 'record' => $record]);
        $this->assertEquals($value, $stored);
    }

    public function testAddKeyValueRecord(){
        $key = "foo";
        $value = "static value";
        $record = 17;
        $this->assertTrue($this->client->add(['key' => $key, 'value' => $value, 'record' => $record]));
        $stored = $this->client->get(['key' => $key, 'record' => $record]);
        $this->assertEquals($value, $stored);
    }

    public function testAddKeyValueRecords(){
        $key = "foo";
        $value = "static value";
        $records = [1, 2, 3];
        $result = $this->client->add(['key' => $key, 'value' => $value, 'record' => $records]);
        $this->assertTrue($result[1]);
        $this->assertTrue($result[2]);
        $this->assertTrue($result[3]);
    }

    public function testAuditKeyRecord() {
        $key = random_string();
        $values = ["one", "two", "three"];
        $record = 1000;
        foreach($values as $value){
            $this->client->set($key, $value, $record);
        }
        $audit = $this->client->audit($key, $record);
        $this->assertEquals(5, count($audit));
        $expected = "ADD";
        foreach($audit as $k => $v){
            $this->assertTrue(str_starts_with($v, $expected));
            $expected = $expected == "ADD" ? "REMOVE" : "ADD";
        }
    }

    public function testAuditKeyRecordStart(){
        $key = random_string();
        $values = ["one", "two", "three"];
        $record = 1000;
        $values = [4, 5, 6];
        foreach($values as $value){
            $this->client->set($key, $value, $record);
        }
        $start = $this->client->time();
        foreach($values as $value){
            $this->client->set($key, $value, $record);
        }
        $audit = $this->client->audit($key, $record, $start);
        $this->assertEquals(6, count($audit));
    }

    public function testAuditKeyRecordStartEnd(){
        $key = random_string();
        $values = ["one", "two", "three"];
        $record = 1000;
        $values = [4, 5, 6];
        foreach($values as $value){
            $this->client->set($key, $value, $record);
        }
        $start = $this->client->time();
        foreach($values as $value){
            $this->client->set($key, $value, $record);
        }
        $end = $this->client->time();
        $values = array(true, false);
        foreach($values as $value){
            $this->client->set($key, $value, $record);
        }
        $audit = $this->client->audit($key, $record, $start, array('end' => $end));
        $this->assertEquals(6, count($audit));
    }

    public function testAuditKeyRecordStartstr(){
        $key = random_string();
        $values = ["one", "two", "three"];
        $record = 1000;
        $values = [4, 5, 6];
        foreach($values as $value){
            $this->client->set($key, $value, $record);
        }
        $anchor = $this->getTimeAnchor();
        foreach($values as $value){
            $this->client->set($key, $value, $record);
        }
        $start = $this->getElapsedMillisString($anchor);
        $audit = $this->client->audit($key, $record, $start);
        $this->assertEquals(6, count($audit));
    }

    public function testAuditKeyRecordStartstrEndstr(){
        $key = random_string();
        $values = ["one", "two", "three"];
        $record = 1000;
        $values = [4, 5, 6];
        foreach($values as $value){
            $this->client->set($key, $value, $record);
        }
        $sanchor = $this->getTimeAnchor();
        foreach($values as $value){
            $this->client->set($key, $value, $record);
        }
        $eanchor = $this->getTimeAnchor();
        $values = array(true, false);
        foreach($values as $value){
            $this->client->set($key, $value, $record);
        }
        $start = $this->getElapsedMillisString($sanchor);
        $end = $this->getElapsedMillisString($eanchor);
        $audit = $this->client->audit($key, $record, $start, array('end' => $end));
        $this->assertEquals(6, count($audit));
    }

    public function testAuditRecord(){
        $key1 = random_string();
        $key2 = random_string();
        $key3 = random_string();
        $value = "foo";
        $record = 1002;
        $this->client->add($key1, $value, $record);
        $this->client->add($key2, $value, $record);
        $this->client->add($key3, $value, $record);
        $audit = $this->client->audit($record);
        $this->assertEquals(3, count($audit));
    }

    public function testAuditRecordStart(){
        $key1 = random_string();
        $key2 = random_string();
        $key3 = random_string();
        $value = "bar";
        $record = 344;
        $this->client->add($key1, $value, $record);
        $this->client->add($key2, $value, $record);
        $this->client->add($key3, $value, $record);
        $start = $this->client->time();
        $this->client->remove($key1, $value, $record);
        $this->client->remove($key2, $value, $record);
        $this->client->remove($key3, $value, $record);
        $audit = $this->client->audit($record, array('start' => $start));
        $this->assertEquals(3, count($audit));
    }

    public function testAuditRecordStartEnd(){
        $key1 = random_string();
        $key2 = random_string();
        $key3 = random_string();
        $value = "bar";
        $record = 344;
        $this->client->add($key1, $value, $record);
        $this->client->add($key2, $value, $record);
        $this->client->add($key3, $value, $record);
        $start = $this->client->time();
        $this->client->remove($key1, $value, $record);
        $this->client->remove($key2, $value, $record);
        $this->client->remove($key3, $value, $record);
        $end = $this->client->time();
        $this->client->add($key1, $value, $record);
        $this->client->add($key2, $value, $record);
        $this->client->add($key3, $value, $record);
        $audit = $this->client->audit($record, array('start' => $start, 'end' => $end));
        $this->assertEquals(3, count($audit));
    }

    public function testAuditRecordStartstr(){
        $key1 = random_string();
        $key2 = random_string();
        $key3 = random_string();
        $value = "bar";
        $record = 344;
        $this->client->add($key1, $value, $record);
        $this->client->add($key2, $value, $record);
        $this->client->add($key3, $value, $record);
        $anchor = $this->getTimeAnchor();
        $this->client->remove($key1, $value, $record);
        $this->client->remove($key2, $value, $record);
        $this->client->remove($key3, $value, $record);
        $start = $this->getElapsedMillisString($anchor);
        $audit = $this->client->audit($record, array('start' => $start));
        $this->assertEquals(3, count($audit));
    }

    public function testAuditRecordStartstrEndstr(){
        $key1 = random_string();
        $key2 = random_string();
        $key3 = random_string();
        $value = "bar";
        $record = 344;
        $this->client->add($key1, $value, $record);
        $this->client->add($key2, $value, $record);
        $this->client->add($key3, $value, $record);
        $sanchor = $this->getTimeAnchor();
        $this->client->remove($key1, $value, $record);
        $this->client->remove($key2, $value, $record);
        $this->client->remove($key3, $value, $record);
        $eanchor = $this->getTimeAnchor();
        $this->client->add($key1, $value, $record);
        $this->client->add($key2, $value, $record);
        $this->client->add($key3, $value, $record);
        $start = $this->getElapsedMillisString($sanchor);
        $end = $this->getElapsedMillisString($eanchor);
        $audit = $this->client->audit($record, array('start' => $start, 'end' => $end));
        $this->assertEquals(3, count($audit));
    }

    public function testBrowseKey(){
        $key = random_string();
        $value = 10;
        $this->client->add($key, $value, array(1, 2, 3));
        $value = random_string();
        $this->client->add($key, $value, array(10, 20, 30));
        $data = $this->client->browse($key);
        $this->assertEquals([1, 2, 3], $data[10]);
        asort($data[$value]);
        $this->assertEquals([10, 20, 30], array_values($data[$value]));
    }

    public function testBrowseKeyTime(){
        $key = random_string();
        $value = 10;
        $this->client->add($key, $value, array(1, 2, 3));
        $value = random_string();
        $this->client->add($key, $value, array(10, 20, 30));
        $time = $this->client->time();
        $this->client->add($key, $value, array(100, 200, 300));
        $data = $this->client->browse($key, array('time' => $time));
        $this->assertEquals([1, 2, 3], $data[10]);
        asort($data[$value]);
        $this->assertEquals([10, 20, 30], array_values($data[$value]));
    }

    public function testBrowseKeyTimestr(){
        $key = random_string();
        $value = 10;
        $this->client->add($key, $value, array(1, 2, 3));
        $value = random_string();
        $this->client->add($key, $value, array(10, 20, 30));
        $anchor = $this->getTimeAnchor();
        $this->client->add($key, $value, array(100, 200, 300));
        $time = $this->getElapsedMillisString($anchor);
        $data = $this->client->browse($key, array('time' => $time));
        $this->assertEquals([1, 2, 3], $data[10]);
        asort($data[$value]);
        $this->assertEquals([10, 20, 30], array_values($data[$value]));
    }

    public function testBrowseKeys(){
        $key1 = random_string();
        $key2 = random_string();
        $key3 = random_string();
        $value1 = "A";
        $value2 = "B";
        $value3 = "C";
        $record1 = 1;
        $record2 = 2;
        $record3 = 3;
        $this->client->add($key1, $value1, $record1);
        $this->client->add($key2, $value2, $record2);
        $this->client->add($key3, $value3, $record3);
        $data = $this->client->browse(array($key1, $key2, $key3));
        $this->assertEquals(array($value1 => array($record1)),$data[$key1]);
        $this->assertEquals(array($value2 => array($record2)),$data[$key2]);
        $this->assertEquals(array($value3 => array($record3)),$data[$key3]);
    }

    public function testBrowseKeysTime(){
        $key1 = random_string();
        $key2 = random_string();
        $key3 = random_string();
        $value1 = "A";
        $value2 = "B";
        $value3 = "C";
        $record1 = 1;
        $record2 = 2;
        $record3 = 3;
        $this->client->add($key1, $value1, $record1);
        $this->client->add($key2, $value2, $record2);
        $this->client->add($key3, $value3, $record3);
        $time = $this->client->time();
        $this->client->add($key1, "Foo");
        $this->client->add($key2, "Foo");
        $this->client->add($key3, "Foo");
        $data = $this->client->browse(array($key1, $key2, $key3), $time);
        $this->assertEquals(array($value1 => array($record1)),$data[$key1]);
        $this->assertEquals(array($value2 => array($record2)),$data[$key2]);
        $this->assertEquals(array($value3 => array($record3)),$data[$key3]);
    }

    public function testBrowseKeysTimestr(){
        $key1 = random_string();
        $key2 = random_string();
        $key3 = random_string();
        $value1 = "A";
        $value2 = "B";
        $value3 = "C";
        $record1 = 1;
        $record2 = 2;
        $record3 = 3;
        $this->client->add($key1, $value1, $record1);
        $this->client->add($key2, $value2, $record2);
        $this->client->add($key3, $value3, $record3);
        $anchor = $this->getTimeAnchor();
        $this->client->add($key1, "Foo");
        $this->client->add($key2, "Foo");
        $this->client->add($key3, "Foo");
        $time = $this->getElapsedMillisString($anchor);
        $data = $this->client->browse(array($key1, $key2, $key3), $time);
        $this->assertEquals(array($value1 => array($record1)),$data[$key1]);
        $this->assertEquals(array($value2 => array($record2)),$data[$key2]);
        $this->assertEquals(array($value3 => array($record3)),$data[$key3]);
    }

    public function testChronologizeKeyRecord(){
        $key = random_string();
        $record = rand();
        $this->client->add($key, 1, $record);
        $this->client->add($key, 2, $record);
        $this->client->add($key, 3, $record);
        $this->client->remove($key, 1, $record);
        $this->client->remove($key, 2, $record);
        $this->client->remove($key, 3, $record);
        $data = $this->client->chronologize(array('key' => $key, 'record' => $record));
        $this->assertEquals(array(array(1), array(1,2), array(1,2,3), array(2,3), array(3)), array_values($data));
    }

    public function testChronologizeKeyRecordStart(){
        $key = random_string();
        $record = rand();
        $this->client->add($key, 1, $record);
        $this->client->add($key, 2, $record);
        $this->client->add($key, 3, $record);
        $start = $this->client->time();
        $this->client->remove($key, 1, $record);
        $this->client->remove($key, 2, $record);
        $this->client->remove($key, 3, $record);
        $data = $this->client->chronologize(array('start' => $start, 'key' => $key, 'record' => $record));
        $this->assertEquals(array(array(2,3), array(3)), array_values($data));
    }

    public function testChronologizeKeyRecordStartstr(){
        $key = random_string();
        $record = rand();
        $this->client->add($key, 1, $record);
        $this->client->add($key, 2, $record);
        $this->client->add($key, 3, $record);
        $anchor = $this->getTimeAnchor();
        $this->client->remove($key, 1, $record);
        $this->client->remove($key, 2, $record);
        $this->client->remove($key, 3, $record);
        $start = $this->getElapsedMillisString($anchor);
        $data = $this->client->chronologize(array('start' => $start, 'key' => $key, 'record' => $record));
        $this->assertEquals(array(array(2,3), array(3)), array_values($data));
    }

    public function testChronologizeKeyRecordStartEnd(){
        $key = random_string();
        $record = rand();
        $this->client->add($key, 1, $record);
        $this->client->add($key, 2, $record);
        $this->client->add($key, 3, $record);
        $start = $this->client->time();
        $this->client->remove($key, 1, $record);
        $end = $this->client->time();
        $this->client->remove($key, 2, $record);
        $this->client->remove($key, 3, $record);
        $data = $this->client->chronologize(array('start' => $start, 'key' => $key, 'record' => $record, 'end' => $end));
        $this->assertEquals(array(array(2,3)), array_values($data));
    }

    public function testChronologizeKeyRecordStartstrEndstr(){
        $key = random_string();
        $record = rand();
        $this->client->add($key, 1, $record);
        $this->client->add($key, 2, $record);
        $this->client->add($key, 3, $record);
        $sanchor = $this->getTimeAnchor();
        $this->client->remove($key, 1, $record);
        $eanchor = $this->getTimeAnchor();
        $this->client->remove($key, 2, $record);
        $this->client->remove($key, 3, $record);
        $start = $this->getElapsedMillisString($sanchor);
        $end = $this->getElapsedMillisString($eanchor);
        $data = $this->client->chronologize(array('start' => $start, 'key' => $key, 'record' => $record, 'end' => $end));
        $this->assertEquals(array(array(2,3)), array_values($data));
    }

    public function testClearKeyRecord(){
        $key = random_string();
        $record = rand();
        $this->client->add($key, 1, $record);
        $this->client->add($key, 2, $record);
        $this->client->add($key, 3, $record);
        $this->client->clear(array('key' => $key, 'record' => $record));
        $data = $this->client->select(array('key' => $key, 'record' => $record));
        $this->assertTrue(empty($data));
    }

    public function testClearKeyRecords(){
        $key = random_string();
        $records = [1, 2, 3];
        $this->client->add($key, 1, $records);
        $this->client->add($key, 2, $records);
        $this->client->add($key, 3, $records);
        $this->client->clear($key, $records);
        $data = $this->client->select(array('key' => $key, 'records' => $records));
        $this->assertTrue(empty($data));
    }

    public function testClearKeysRecord(){
        $key1 = "key1";
        $key2 = "key2";
        $key3 = "key3";
        $record = rand();
        $this->client->add($key1, 1, $record);
        $this->client->add($key2, 2, $record);
        $this->client->add($key3, 3, $record);
        $this->client->clear(array($key1, $key2, $key3), $record);
        $data = $this->client->select(array('keys' => [$key1, $key2, $key3], 'record' => $record));
        $this->assertTrue(empty($data));
    }

    public function testClearKeysRecords(){
        $data = [
            'a' => 'A',
            'b' => 'B',
            'c' => ["C", true],
            'd' => 'D'
        ];
        $records = [1, 2, 3];
        $this->client->insert($data, $records);
        $this->client->clear(['a, b, c'], $records);
        $data = $this->client->get(['key' => 'd', 'records' => $records]);
        $this->assertEquals([1 => 'D', 2 => "D", 3 => 'D'], $data);
    }

    public function testClearRecord(){
        $data = [
            'a' => 'A',
            'b' => 'B',
            'c' => ["C", true],
            'd' => 'D'
        ];
        $record = $this->client->insert(array('data' => $data))[0];
        $this->client->clear($record);
        $data = $this->client->select(array('record' => $record));
        $this->assertTrue(empty($data));
    }

    public function testCommit(){
        $this->client->stage();
        $record = $this->client->add("name", "jeff nelson");
        $this->assertTrue($this->client->commit());
        $this->assertEquals(["name"], $this->client->describe($record));
    }

    public function testDescribeRecord(){
        $this->client->set("name", "tom brady", 1);
        $this->client->set("age", 100, 1);
        $this->client->set("team", "new england patriots", 1);
        $keys = $this->client->describe(1);
        $expected = ["name", "age", "team"];
        sort($expected);
        sort($keys);
        $this->assertEquals($expected, $keys);
    }

    public function testDescribeRecordTime(){
        $this->client->set("name", "tom brady", 1);
        $this->client->set("age", 100, 1);
        $this->client->set("team", "new england patriots", 1);
        $time = $this->client->time();
        $this->client->clear("name", 1);
        $keys = $this->client->describe(1, $time);
        $expected = ["name", "age", "team"];
        sort($expected);
        sort($keys);
        $this->assertEquals($expected, $keys);
    }

    public function testDescribeRecordTimestr(){
        $this->client->set("name", "tom brady", 1);
        $this->client->set("age", 100, 1);
        $this->client->set("team", "new england patriots", 1);
        $anchor = $this->getTimeAnchor();
        $this->client->clear("name", 1);
        $time = $this->getElapsedMillisString($anchor);
        $keys = $this->client->describe(1, $time);
        $expected = ["name", "age", "team"];
        sort($expected);
        sort($keys);
        $this->assertEquals($expected, $keys);
    }

    public function testDescribeRecords(){
        $records = [1, 2, 3];
        $this->client->set("name", "tom brady", $records);
        $this->client->set("age", 100, $records);
        $this->client->set("team", "new england patriots", $records);
        $keys = $this->client->describe($records);
        $expected = ["name", "age", "team"];
        sort($expected);
        sort($keys[1]);
        sort($keys[2]);
        sort($keys[3]);
        $this->assertEquals($expected, $keys[1]);
        $this->assertEquals($expected, $keys[2]);
        $this->assertEquals($expected, $keys[3]);
    }

    public function testDescribeRecordsTime(){
        $records = [1, 2, 3];
        $this->client->set("name", "tom brady", $records);
        $this->client->set("age", 100, $records);
        $this->client->set("team", "new england patriots", $records);
        $time = $this->client->time();
        $this->client->clear($records);
        $keys = $this->client->describe($records, $time);
        $expected = ["name", "age", "team"];
        sort($expected);
        sort($keys[1]);
        sort($keys[2]);
        sort($keys[3]);
        $this->assertEquals($expected, $keys[1]);
        $this->assertEquals($expected, $keys[2]);
        $this->assertEquals($expected, $keys[3]);
    }

    public function testDescribeRecordsTimestr(){
        $records = [1, 2, 3];
        $this->client->set("name", "tom brady", $records);
        $this->client->set("age", 100, $records);
        $this->client->set("team", "new england patriots", $records);
        $anchor = $this->getTimeAnchor();
        $this->client->clear($records);
        $time = $this->getElapsedMillisString($anchor);
        $keys = $this->client->describe($records, $time);
        $expected = ["name", "age", "team"];
        sort($expected);
        sort($keys[1]);
        sort($keys[2]);
        sort($keys[3]);
        $this->assertEquals($expected, $keys[1]);
        $this->assertEquals($expected, $keys[2]);
        $this->assertEquals($expected, $keys[3]);
    }

    public function testDiffKeyRecordStart(){
        $key = random_string();
        $record = rand();
        $this->client->add($key, 1, $record);
        $start = $this->client->time();
        $this->client->add($key, 2, $record);
        $this->client->remove($key, 1, $record);
        $diff = $this->client->diff($key, ['record' => $record, 'start' => $start]);
        $this->assertEquals([2], $diff[Diff::ADDED]);
        $this->assertEquals([1], $diff[Diff::REMOVED]);
    }

    public function testDiffKeyRecordStartstr(){
        $key = random_string();
        $record = rand();
        $this->client->add($key, 1, $record);
        $anchor = $this->getTimeAnchor();
        $this->client->add($key, 2, $record);
        $this->client->remove($key, 1, $record);
        $start = $this->getElapsedMillisString($anchor);
        $diff = $this->client->diff(['key' => $key, 'record' => $record, 'start' => $start]);
        $this->assertEquals([2], $diff[Diff::ADDED]);
        $this->assertEquals([1], $diff[Diff::REMOVED]);
    }

    public function testDiffKeyRecordStartEnd(){
        $key = random_string();
        $record = rand();
        $this->client->add($key, 1, $record);
        $start = $this->client->time();
        $this->client->add($key, 2, $record);
        $this->client->remove($key, 1, $record);
        $end = $this->client->time();
        $this->client->set($key, 3, $record);
        $diff = $this->client->diff(['key' => $key, 'record' => $record, 'start' => $start, 'end' => $end]);
        $this->assertEquals([2], $diff[Diff::ADDED]);
        $this->assertEquals([1], $diff[Diff::REMOVED]);
    }

    public function testDiffKeyRecordStartstrEndstr(){
        $key = random_string();
        $record = rand();
        $this->client->add($key, 1, $record);
        $sanchor = $this->getTimeAnchor();
        $this->client->add($key, 2, $record);
        $this->client->remove($key, 1, $record);
        $eanchor = $this->getTimeAnchor();
        $this->client->set($key, 3, $record);
        $start = $this->getElapsedMillisString($sanchor);
        $end = $this->getElapsedMillisString($eanchor);
        $diff = $this->client->diff(['key' => $key, 'record' => $record, 'start' => $start, 'end' => $end]);
        $this->assertEquals([2], $diff[Diff::ADDED]);
        $this->assertEquals([1], $diff[Diff::REMOVED]);
    }

    public function testDiffKeyStart(){
        $key = random_string();
        $this->client->add($key, 1, 1);
        $start = $this->client->time();
        $this->client->add($key, 2, 1);
        $this->client->add($key, 1, 2);
        $this->client->add($key, 3, 3);
        $this->client->remove($key, 1, 2);
        $diff = $this->client->diff(['key' => $key, 'start' => $start]);
        $this->assertEquals(2, count($diff));
        $diff2 = $diff[2];
        $diff3 = $diff[3];
        $this->assertEquals([1], $diff2[Diff::ADDED]);
        $this->assertEquals([3], $diff3[Diff::ADDED]);
        $this->assertNull($diff2[Diff::REMOVED]);
        $this->assertNull($diff3[Diff::REMOVED]);
    }

    public function testDiffKeyStartstr(){
        $key = random_string();
        $this->client->add($key, 1, 1);
        $anchor = $this->getTimeAnchor();
        $this->client->add($key, 2, 1);
        $this->client->add($key, 1, 2);
        $this->client->add($key, 3, 3);
        $this->client->remove($key, 1, 2);
        $start = $this->getElapsedMillisString($anchor);
        $diff = $this->client->diff(['key' => $key, 'start' => $start]);
        $this->assertEquals(2, count($diff));
        $diff2 = $diff[2];
        $diff3 = $diff[3];
        $this->assertEquals([1], $diff2[Diff::ADDED]);
        $this->assertEquals([3], $diff3[Diff::ADDED]);
        $this->assertNull($diff2[Diff::REMOVED]);
        $this->assertNull($diff3[Diff::REMOVED]);
    }

    public function testDiffKeyStartEnd(){
        $key = random_string();
        $this->client->add($key, 1, 1);
        $start = $this->client->time();
        $this->client->add($key, 2, 1);
        $this->client->add($key, 1, 2);
        $this->client->add($key, 3, 3);
        $this->client->remove($key, 1, 2);
        $end = $this->client->time();
        $this->client->add($key, 4, 1);
        $diff = $this->client->diff(['key' => $key, 'start' => $start, 'end' => $end]);
        $this->assertEquals(2, count($diff));
        $diff2 = $diff[2];
        $diff3 = $diff[3];
        $this->assertEquals([1], $diff2[Diff::ADDED]);
        $this->assertEquals([3], $diff3[Diff::ADDED]);
        $this->assertNull($diff2[Diff::REMOVED]);
        $this->assertNull($diff3[Diff::REMOVED]);
    }

    public function testDiffKeyStartstrEndstr(){
        $key = random_string();
        $this->client->add($key, 1, 1);
        $sanchor = $this->getTimeAnchor();
        $this->client->add($key, 2, 1);
        $this->client->add($key, 1, 2);
        $this->client->add($key, 3, 3);
        $this->client->remove($key, 1, 2);
        $eanchor = $this->getTimeAnchor();
        $this->client->add($key, 4, 1);
        $start = $this->getElapsedMillisString($sanchor);
        $end = $this->getElapsedMillisString($eanchor);
        $diff = $this->client->diff(['key' => $key, 'start' => $start, 'end' => $end]);
        $this->assertEquals(2, count($diff));
        $diff2 = $diff[2];
        $diff3 = $diff[3];
        $this->assertEquals([1], $diff2[Diff::ADDED]);
        $this->assertEquals([3], $diff3[Diff::ADDED]);
        $this->assertNull($diff2[Diff::REMOVED]);
        $this->assertNull($diff3[Diff::REMOVED]);
    }

    public function testDiffRecordStart(){
        $this->client->add("foo", 1, 1);
        $start = $this->client->time();
        $this->client->set("foo", 2, 1);
        $this->client->add("bar", true, 1);
        $diff = $this->client->diff(['record' => 1, 'start' => $start]);
        $this->assertEquals([1], $diff['foo'][Diff::REMOVED]);
        $this->assertEquals([2], $diff['foo'][Diff::ADDED]);
        $this->assertEquals([true], $diff['bar'][Diff::ADDED]);
    }

    public function testDiffRecordStartstr(){
        $this->client->add("foo", 1, 1);
        $anchor = $this->getTimeAnchor();
        $this->client->set("foo", 2, 1);
        $this->client->add("bar", true, 1);
        $start = $this->getElapsedMillisString($anchor);
        $diff = $this->client->diff(['record' => 1, 'start' => $start]);
        $this->assertEquals([1], $diff['foo'][Diff::REMOVED]);
        $this->assertEquals([2], $diff['foo'][Diff::ADDED]);
        $this->assertEquals([true], $diff['bar'][Diff::ADDED]);
    }

    public function testDiffRecordStartEnd(){
        $this->client->add("foo", 1, 1);
        $start = $this->client->time();
        $this->client->set("foo", 2, 1);
        $this->client->add("bar", true, 1);
        $end = $this->client->time();
        $this->client->set("car", 100, 1);
        $diff = $this->client->diff(['record' => 1, 'start' => $start, 'end' => $end]);
        $this->assertEquals([1], $diff['foo'][Diff::REMOVED]);
        $this->assertEquals([2], $diff['foo'][Diff::ADDED]);
        $this->assertEquals([true], $diff['bar'][Diff::ADDED]);
    }

    public function testDiffRecordStartstrEndstr(){
        $this->client->add("foo", 1, 1);
        $sanchor = $this->getTimeAnchor();
        $this->client->set("foo", 2, 1);
        $this->client->add("bar", true, 1);
        $eanchor = $this->getTimeAnchor();
        $this->client->set("car", 100, 1);
        $start = $this->getElapsedMillisString($sanchor);
        $end = $this->getElapsedMillisString($eanchor);
        $diff = $this->client->diff(['record' => 1, 'start' => $start, 'end' => $end]);
        $this->assertEquals([1], $diff['foo'][Diff::REMOVED]);
        $this->assertEquals([2], $diff['foo'][Diff::ADDED]);
        $this->assertEquals([true], $diff['bar'][Diff::ADDED]);
    }

    public function testFindCcl(){
        $key = random_string();
        foreach(range(1, 10) as $i){
            $this->client->add($key, $i, $i);
        }
        $records = $this->client->find("$key > 3");
        $this->assertEquals(range(4, 10), $records);
    }

    public function testFindCclHandleParseException(){
        $this->setExpectedException('Concourse\Thrift\Exceptions\ParseException');
        $this->client->find("throw parse exception");
    }

    public function testFindKeyOperatorValue(){
        $key = random_string();
        foreach(range(1, 10) as $i){
            $this->client->add($key, $i, $i);
        }
        $records = $this->client->find($key, Operator::EQUALS, 5);
        $this->assertEquals([5], $records);
    }

    public function testFindKeyOperatorValues(){
        $key = random_string();
        foreach(range(1, 10) as $i){
            $this->client->add($key, $i, $i);
        }
        $records = $this->client->find($key, Operator::BETWEEN, [3,6]);
        $this->assertEquals([3,4,5], $records);
    }

    public function testFindKeyOperatorValuesTime(){
        $key = random_string();
        foreach(range(1, 10) as $i){
            $this->client->add($key, $i, $i);
        }
        $time = $this->client->time();
        foreach(range(1, 10) as $i){
            $this->client->add($key, $i, $i+1);
        }
        $records = $this->client->find($key, Operator::BETWEEN, [3,6], ['time' => $time]);
        $this->assertEquals([3,4,5], $records);
    }

    public function testFindKeyOperatorValuesTimestr(){
        $key = random_string();
        foreach(range(1, 10) as $i){
            $this->client->add($key, $i, $i);
        }
        $anchor = $this->getTimeAnchor();
        foreach(range(1, 10) as $i){
            $this->client->add($key, $i, $i+1);
        }
        $time = $this->getElapsedMillisString($anchor);
        $records = $this->client->find($key, Operator::BETWEEN, [3,6], ['time' => $time]);
        $this->assertEquals([3,4,5], $records);
    }

    public function testFindKeyOperatorstrValuesTime(){
        $key = random_string();
        foreach(range(1, 10) as $i){
            $this->client->add($key, $i, $i);
        }
        $time = $this->client->time();
        foreach(range(1, 10) as $i){
            $this->client->add($key, $i, $i+1);
        }
        $records = $this->client->find($key, "bw", [3,6], ['time' => $time]);
        $this->assertEquals([3,4,5], $records);
    }

    public function testFindKeyOperatorstrValuesTimestr(){
        $key = random_string();
        foreach(range(1, 10) as $i){
            $this->client->add($key, $i, $i);
        }
        $anchor = $this->getTimeAnchor();
        foreach(range(1, 10) as $i){
            $this->client->add($key, $i, $i+1);
        }
        $time = $this->getElapsedMillisString($anchor);
        $records = $this->client->find($key, "bw", [3,6], ['time' => $time]);
        $this->assertEquals([3,4,5], $records);
    }

    public function testFindKeyOperatorstrValue(){
        $key = random_string();
        foreach(range(1, 10) as $i){
            $this->client->add($key, $i, $i);
        }
        $records = $this->client->find($key, "=", 5);
        $this->assertEquals([5], $records);
    }

    public function testFindKeyOperatorstrValues(){
        $key = random_string();
        foreach(range(1, 10) as $i){
            $this->client->add($key, $i, $i);
        }
        $records = $this->client->find($key, "bw", [3,6]);
        $this->assertEquals([3,4,5], $records);
    }

    public function testFindKeyOperatorValueTime(){
        $key = random_string();
        foreach(range(1, 10) as $i){
            $this->client->add($key, $i, $i);
        }
        $time = $this->client->time();
        foreach(range(1, 10) as $i){
            $this->client->add($key, 5, $i);
        }
        $records = $this->client->find($key, Operator::EQUALS, 5, ['time' => $time]);
        $this->assertEquals([5], $records);
    }

    public function testFindKeyOperatorstrValueTime(){
        $key = random_string();
        foreach(range(1, 10) as $i){
            $this->client->add($key, $i, $i);
        }
        $time = $this->client->time();
        foreach(range(1, 10) as $i){
            $this->client->add($key, 5, $i);
        }
        $records = $this->client->find($key, "=", 5, ['time' => $time]);
        $this->assertEquals([5], $records);
    }

    public function testFindKeyOperatorValueTimestr(){
        $key = random_string();
        foreach(range(1, 10) as $i){
            $this->client->add($key, $i, $i);
        }
        $anchor = $this->getTimeAnchor();
        foreach(range(1, 10) as $i){
            $this->client->add($key, 5, $i);
        }
        $time = $this->getElapsedMillisString($anchor);
        $records = $this->client->find($key, Operator::EQUALS, 5, ['time' => $time]);
        $this->assertEquals([5], $records);
    }

    public function testFindKeyOperatorstrValueTimestr(){
        $key = random_string();
        foreach(range(1, 10) as $i){
            $this->client->add($key, $i, $i);
        }
        $anchor = $this->getTimeAnchor();
        foreach(range(1, 10) as $i){
            $this->client->add($key, 5, $i);
        }
        $time = $this->getElapsedMillisString($anchor);
        $records = $this->client->find($key, "=", 5, ['time' => $time]);
        $this->assertEquals([5], $records);
    }

    public function testGetCcl(){
        $key1 = random_string();
        $key2 = random_string();
        $record1 = rand();
        $record2 = rand();
        $this->client->add($key1, 1, [$record1, $record2]);
        $this->client->add($key1, 2, [$record1, $record2]);
        $this->client->add($key1, 3, [$record1, $record2]);
        $this->client->add($key2, 10, [$record1, $record2]);
        $ccl = "$key2 = 10";
        $data = $this->client->get(['ccl' => $ccl]);
        $expected = [$key1 => 3, $key2 => 10];
        $this->assertEquals($expected, $data[$record1]);
        $this->assertEquals($expected, $data[$record2]);
    }

    public function testGetCclTime(){
        $key1 = random_string();
        $key2 = random_string();
        $record1 = rand();
        $record2 = rand();
        $this->client->add($key1, 1, [$record1, $record2]);
        $this->client->add($key1, 2, [$record1, $record2]);
        $this->client->add($key1, 3, [$record1, $record2]);
        $this->client->add($key2, 10, [$record1, $record2]);
        $time = $this->client->time();
        $this->client->add($key2, 11, [$record1, $record2]);
        $ccl = "$key2 = 10";
        $data = $this->client->get(['ccl' => $ccl, 'time' => $time]);
        $expected = [$key1 => 3, $key2 => 10];
        $this->assertEquals($expected, $data[$record1]);
        $this->assertEquals($expected, $data[$record2]);
    }

    public function testGetCclTimetr(){
        $key1 = random_string();
        $key2 = random_string();
        $record1 = rand();
        $record2 = rand();
        $this->client->add($key1, 1, [$record1, $record2]);
        $this->client->add($key1, 2, [$record1, $record2]);
        $this->client->add($key1, 3, [$record1, $record2]);
        $this->client->add($key2, 10, [$record1, $record2]);
        $anchor = $this->getTimeAnchor();
        $this->client->add($key2, 11, [$record1, $record2]);
        $ccl = "$key2 = 10";
        $time = $this->getElapsedMillisString($anchor);
        $data = $this->client->get(['ccl' => $ccl, 'time' => $time]);
        $expected = [$key1 => 3, $key2 => 10];
        $this->assertEquals($expected, $data[$record1]);
        $this->assertEquals($expected, $data[$record2]);
    }

    public function testGetKeyCcl(){
        $key1 = random_string();
        $key2 = random_string();
        $record1 = rand();
        $record2 = rand();
        $this->client->add($key1, 1, [$record1, $record2]);
        $this->client->add($key1, 2, [$record1, $record2]);
        $this->client->add($key1, 3, [$record1, $record2]);
        $this->client->add($key2, 10, [$record1, $record2]);
        $this->client->add($key1, 4, [$record2]);
        $ccl = "$key2 = 10";
        $data = $this->client->get(['ccl' => $ccl, 'key' => $key1]);
        $expected = [$record1 => 3, $record2 => 4];
        $this->assertEquals($expected, $data);
    }

    public function testGetKeyCclTime(){
        $key1 = random_string();
        $key2 = random_string();
        $record1 = rand();
        $record2 = rand();
        $this->client->add($key1, 1, [$record1, $record2]);
        $this->client->add($key1, 2, [$record1, $record2]);
        $this->client->add($key1, 3, [$record1, $record2]);
        $this->client->add($key2, 10, [$record1, $record2]);
        $this->client->add($key1, 4, [$record2]);
        $time = $this->client->time();
        $this->client->set($key1, 100, [$record2, $record1]);
        $ccl = "$key2 = 10";
        $data = $this->client->get(['ccl' => $ccl, 'key' => $key1, 'time' => $time]);
        $expected = [$record1 => 3, $record2 => 4];
        $this->assertEquals($expected, $data);
    }

    public function testGetKeyCclTimestr(){
        $key1 = random_string();
        $key2 = random_string();
        $record1 = rand();
        $record2 = rand();
        $this->client->add($key1, 1, [$record1, $record2]);
        $this->client->add($key1, 2, [$record1, $record2]);
        $this->client->add($key1, 3, [$record1, $record2]);
        $this->client->add($key2, 10, [$record1, $record2]);
        $this->client->add($key1, 4, [$record2]);
        $anchor = $this->getTimeAnchor();
        $this->client->set($key1, 100, [$record2, $record1]);
        $ccl = "$key2 = 10";
        $time = $this->getElapsedMillisString($anchor);
        $data = $this->client->get(['ccl' => $ccl, 'key' => $key1, 'time' => $time]);
        $expected = [$record1 => 3, $record2 => 4];
        $this->assertEquals($expected, $data);
    }

    public function testGetKeysCcl(){
        $key1 = random_string();
        $key2 = random_string();
        $record1 = rand();
        $record2 = rand();
        $this->client->add($key1, 1, [$record1, $record2]);
        $this->client->add($key1, 2, [$record1, $record2]);
        $this->client->add($key1, 3, [$record1, $record2]);
        $this->client->add($key2, 10, [$record1, $record2]);
        $this->client->add($key1, 4, [$record2]);
        $ccl = "$key2 = 10";
        $data = $this->client->get(['ccl' => $ccl, 'key' => [$key1, $key2]]);
        $expected = [$record1 => [$key1 => 3, $key2 => 10], $record2 => [$key1 => 4, $key2 => 10]];
        $this->assertEquals($expected, $data);
    }

    public function testGetKeysCclTime(){
        $key1 = random_string();
        $key2 = random_string();
        $record1 = rand();
        $record2 = rand();
        $this->client->add($key1, 1, [$record1, $record2]);
        $this->client->add($key1, 2, [$record1, $record2]);
        $this->client->add($key1, 3, [$record1, $record2]);
        $this->client->add($key2, 10, [$record1, $record2]);
        $this->client->add($key1, 4, [$record2]);
        $time = $this->client->time();
        $this->client->set($key1, 100, [$record2]);
        $ccl = "$key2 = 10";
        $data = $this->client->get(['ccl' => $ccl, 'key' => [$key1, $key2], 'time' => $time]);
        $expected = [$record1 => [$key1 => 3, $key2 => 10], $record2 => [$key1 => 4, $key2 => 10]];
        $this->assertEquals($expected, $data);
    }

    public function testGetKeysCclTimestr(){
        $key1 = random_string();
        $key2 = random_string();
        $record1 = rand();
        $record2 = rand();
        $this->client->add($key1, 1, [$record1, $record2]);
        $this->client->add($key1, 2, [$record1, $record2]);
        $this->client->add($key1, 3, [$record1, $record2]);
        $this->client->add($key2, 10, [$record1, $record2]);
        $this->client->add($key1, 4, [$record2]);
        $anchor = $this->getTimeAnchor();
        $this->client->set($key1, 100, [$record2]);
        $ccl = "$key2 = 10";
        $time = $this->getElapsedMillisString($anchor);
        $data = $this->client->get(['ccl' => $ccl, 'key' => [$key1, $key2], 'time' => $time]);
        $expected = [$record1 => [$key1 => 3, $key2 => 10], $record2 => [$key1 => 4, $key2 => 10]];
        $this->assertEquals($expected, $data);
    }

    public function testGetKeyRecord(){
        $this->client->add("foo", 1, 1);
        $this->client->add("foo", 2, 1);
        $this->client->add("foo", 3, 1);
        $this->assertEquals(3, $this->client->get("foo", ['record' => 1]));
    }

    public function testGetKeyRecordTime(){
        $this->client->add("foo", 1, 1);
        $this->client->add("foo", 2, 1);
        $this->client->add("foo", 3, 1);
        $time = $this->client->time();
        $this->client->add("foo", 4, 1);
        $this->assertEquals(3, $this->client->get("foo", ['record' => 1, 'time' => $time]));
    }

    public function testGetKeyRecordTimestr(){
        $this->client->add("foo", 1, 1);
        $this->client->add("foo", 2, 1);
        $this->client->add("foo", 3, 1);
        $anchor = $this->getTimeAnchor();
        $this->client->add("foo", 4, 1);
        $time = $this->getElapsedMillisString($anchor);
        $this->assertEquals(3, $this->client->get("foo", ['record' => 1, 'time' => $time]));
    }

    public function testGetKeyRecords(){
        $this->client->add("foo", 1, [1, 2, 3]);
        $this->client->add("foo", 2, [1, 2, 3]);
        $this->client->add("foo", 3, [1, 2, 3]);
        $this->assertEquals([1 => 3, 2 => 3, 3 => 3], $this->client->get("foo", ['record' => [1, 2, 3]]));
    }

    public function testGetKeyRecordsTime(){
        $this->client->add("foo", 1, [1, 2, 3]);
        $this->client->add("foo", 2, [1, 2, 3]);
        $this->client->add("foo", 3, [1, 2, 3]);
        $time = $this->client->time();
        $this->client->add("foo", 4, [1, 2, 3]);
        $this->assertEquals([1 => 3, 2 => 3, 3 => 3], $this->client->get("foo", ['record' => [1, 2, 3], 'time' => $time]));
    }

    public function testGetKeyRecordsTimestr(){
        $this->client->add("foo", 1, [1, 2, 3]);
        $this->client->add("foo", 2, [1, 2, 3]);
        $this->client->add("foo", 3, [1, 2, 3]);
        $anchor = $this->getTimeAnchor();
        $this->client->add("foo", 4, [1, 2, 3]);
        $time = $this->getElapsedMillisString($anchor);
        $this->assertEquals([1 => 3, 2 => 3, 3 => 3], $this->client->get("foo", ['record' => [1, 2, 3], 'time' => $time]));
    }

    public function testGetKeysRecord(){
        $this->client->add("foo", 1, 1);
        $this->client->add("foo", 2, 1);
        $this->client->add("bar", 1, 1);
        $this->client->add("bar", 2, 1);
        $data = $this->client->get(['keys' => ["foo", "bar"], 'record' => 1]);
        $expected = ['foo' => 2, 'bar' => 2];
        $this->assertEquals($expected, $data);
    }

    public function testGetKeysRecordTime(){
        $this->client->add("foo", 1, 1);
        $this->client->add("foo", 2, 1);
        $this->client->add("bar", 1, 1);
        $this->client->add("bar", 2, 1);
        $time = $this->client->time();
        $this->client->add("foo", 3, 1);
        $this->client->add("bar", 3, 1);
        $data = $this->client->get(['keys' => ["foo", "bar"], 'record' => 1, 'time' => $time]);
        $expected = ['foo' => 2, 'bar' => 2];
        $this->assertEquals($expected, $data);
    }

    public function testGetKeysRecordTimestr(){
        $this->client->add("foo", 1, 1);
        $this->client->add("foo", 2, 1);
        $this->client->add("bar", 1, 1);
        $this->client->add("bar", 2, 1);
        $anchor = $this->getTimeAnchor();
        $this->client->add("foo", 3, 1);
        $this->client->add("bar", 3, 1);
        $time = $this->getElapsedMillisString($anchor);
        $data = $this->client->get(['keys' => ["foo", "bar"], 'record' => 1, 'time' => $time]);
        $expected = ['foo' => 2, 'bar' => 2];
        $this->assertEquals($expected, $data);
    }

    public function testGetKeysRecords(){
        $this->client->add("foo", 1, [1, 2]);
        $this->client->add("foo", 2, [1, 2]);
        $this->client->add("bar", 1, [1, 2]);
        $this->client->add("bar", 2, [1, 2]);
        $data = $this->client->get(['keys' => ['foo', 'bar'], 'records' => [1, 2]]);
        $expected = ['foo' => 2, 'bar' => 2];
        $this->assertEquals([1 => $expected, 2 => $expected], $data);
    }

    public function testGetKeysRecordsTime(){
        $this->client->add("foo", 1, [1, 2]);
        $this->client->add("foo", 2, [1, 2]);
        $this->client->add("bar", 1, [1, 2]);
        $this->client->add("bar", 2, [1, 2]);
        $time = $this->client->time();
        $this->client->add("foo", 3, [1, 2]);
        $this->client->add("bar", 3, [1, 2]);
        $data = $this->client->get(['keys' => ['foo', 'bar'], 'records' => [1, 2], 'time' => $time]);
        $expected = ['foo' => 2, 'bar' => 2];
        $this->assertEquals([1 => $expected, 2 => $expected], $data);
    }

    public function testGetKeysRecordsTimestr(){
        $this->client->add("foo", 1, [1, 2]);
        $this->client->add("foo", 2, [1, 2]);
        $this->client->add("bar", 1, [1, 2]);
        $this->client->add("bar", 2, [1, 2]);
        $anchor = $this->getTimeAnchor();
        $this->client->add("foo", 3, [1, 2]);
        $this->client->add("bar", 3, [1, 2]);
        $time = $this->getElapsedMillisString($anchor);
        $data = $this->client->get(['keys' => ['foo', 'bar'], 'records' => [1, 2], 'time' => $time]);
        $expected = ['foo' => 2, 'bar' => 2];
        $this->assertEquals([1 => $expected, 2 => $expected], $data);
    }

    public function testInsertArray(){
        $data = [
            'string' => 'a',
            'int' => 1,
            'double' => 3.14,
            'bool' => true,
            'multi' => ["a", 1, 3.14, true]
        ];
        $record = $this->client->insert(['data' => $data])[0];
        $this->assertEquals("a", $this->client->get("string", ['record' => $record]));
        $this->assertEquals(1, $this->client->get("int", ['record' => $record]));
        $this->assertEquals(true, $this->client->get("bool", ['record' => $record]));
        $this->assertEquals(3.14, $this->client->get("double", ['record' => $record]));
        $this->assertEquals(["a", 1, 3.14, true], $this->client->select([
            'key' => "multi", 'record' => $record]));
    }

    public function testInsertObject(){
        $data = new stdClass();
        $data->string = "a";
        $data->int = 1;
        $data->double = 3.14;
        $data->bool = true;
        $data->multi = ["a", 1, 3.14, true];
        $record = $this->client->insert(['data' => $data])[0];
        $this->assertEquals("a", $this->client->get("string", ['record' => $record]));
        $this->assertEquals(1, $this->client->get("int", ['record' => $record]));
        $this->assertEquals(true, $this->client->get("bool", ['record' => $record]));
        $this->assertEquals(3.14, $this->client->get("double", ['record' => $record]));
        $this->assertEquals(["a", 1, 3.14, true], $this->client->select([
            'key' => "multi", 'record' => $record]));
    }

    public function testInsertJson(){
        $data = '{"string":"a","int":1,"double":3.14,"bool":true,"multi":["a",1,3.14,true]}';
        $record = $this->client->insert(['data' => $data])[0];
        $this->assertEquals("a", $this->client->get("string", ['record' => $record]));
        $this->assertEquals(1, $this->client->get("int", ['record' => $record]));
        $this->assertEquals(true, $this->client->get("bool", ['record' => $record]));
        $this->assertEquals(3.14, $this->client->get("double", ['record' => $record]));
        $this->assertEquals(["a", 1, 3.14, true], $this->client->select([
            'key' => "multi", 'record' => $record]));
    }

    public function testInsertArrays(){
        $data = [
            ['foo' => 1],
            ['foo' => 2],
            ['foo' => 3]
        ];
        $records = $this->client->insert(['data' => $data]);
        $this->assertEquals(count($data), count($records));
    }

    public function testInsertObjects(){
        $obj1 = new stdClass();
        $obj2 = new stdClass();
        $obj3 = new stdClass();
        $obj1->foo = 1;
        $obj2->foo = 2;
        $obj3->foo = 3;
        $data = [$obj1, $obj2, $obj3];
        $records = $this->client->insert(['data' => $data]);
        $this->assertEquals(count($data), count($records));
    }

    public function testInsertJsonList(){
        $data = [
            ['foo' => 1],
            ['foo' => 2],
            ['foo' => 3]
        ];
        $count = count($data);
        $data = json_encode($data);
        $records = $this->client->insert(['data' => $data]);
        $this->assertEquals($count, count($records));
    }

    public function testInsertArrayRecord(){
        $data = [
            'string' => 'a',
            'int' => 1,
            'double' => 3.14,
            'bool' => true,
            'multi' => ["a", 1, 3.14, true]
        ];
        $record = rand();
        $this->assertTrue($this->client->insert(['data' => $data, 'record' => $record]));
        $this->assertEquals("a", $this->client->get("string", ['record' => $record]));
        $this->assertEquals(1, $this->client->get("int", ['record' => $record]));
        $this->assertEquals(true, $this->client->get("bool", ['record' => $record]));
        $this->assertEquals(3.14, $this->client->get("double", ['record' => $record]));
        $this->assertEquals(["a", 1, 3.14, true], $this->client->select([
            'key' => "multi", 'record' => $record]));
    }

    public function testInsertObjectRecord(){
        $data = new stdClass();
        $data->string = "a";
        $data->int = 1;
        $data->double = 3.14;
        $data->bool = true;
        $data->multi = ["a", 1, 3.14, true];
        $record = rand();
        $this->assertTrue($this->client->insert(['data' => $data, 'record' => $record]));
        $this->assertEquals("a", $this->client->get("string", ['record' => $record]));
        $this->assertEquals(1, $this->client->get("int", ['record' => $record]));
        $this->assertEquals(true, $this->client->get("bool", ['record' => $record]));
        $this->assertEquals(3.14, $this->client->get("double", ['record' => $record]));
        $this->assertEquals(["a", 1, 3.14, true], $this->client->select([
            'key' => "multi", 'record' => $record]));
    }

    public function testInsertJsonRecord(){
        $data = '{"string":"a","int":1,"double":3.14,"bool":true,"multi":["a",1,3.14,true]}';
        $record = rand();
        $this->assertTrue($this->client->insert(['data' => $data, 'record' => $record]));
        $this->assertEquals("a", $this->client->get("string", ['record' => $record]));
        $this->assertEquals(1, $this->client->get("int", ['record' => $record]));
        $this->assertEquals(true, $this->client->get("bool", ['record' => $record]));
        $this->assertEquals(3.14, $this->client->get("double", ['record' => $record]));
        $this->assertEquals(["a", 1, 3.14, true], $this->client->select([
            'key' => "multi", 'record' => $record]));
    }

    public function testInsertArrayRecords(){
        $data = [
            'string' => 'a',
            'int' => 1,
            'double' => 3.14,
            'bool' => true,
            'multi' => ["a", 1, 3.14, true]
        ];
        $record = [rand(), rand(), rand()];
        $result = $this->client->insert(['data' => $data, 'record' => $record]);
        $this->assertTrue($result[$record[0]]);
        $this->assertTrue($result[$record[1]]);
        $this->assertTrue($result[$record[2]]);
    }

    public function testInsertObjectRecords(){
        $data = new stdClass();
        $data->string = "a";
        $data->int = 1;
        $data->double = 3.14;
        $data->bool = true;
        $data->multi = ["a", 1, 3.14, true];
        $record = [rand(), rand(), rand()];
        $result = $this->client->insert(['data' => $data, 'record' => $record]);
        $this->assertTrue($result[$record[0]]);
        $this->assertTrue($result[$record[1]]);
        $this->assertTrue($result[$record[2]]);
    }

    public function testInsertJsonRecords(){
        $data = '{"string":"a","int":1,"double":3.14,"bool":true,"multi":["a",1,3.14,true]}';
        $record = [rand(), rand(), rand()];
        $result = $this->client->insert(['data' => $data, 'record' => $record]);
        $this->assertTrue($result[$record[0]]);
        $this->assertTrue($result[$record[1]]);
        $this->assertTrue($result[$record[2]]);
    }

    public function testInventory(){
        $records = [1, 2, 3, 4, 5, 6, 7];
        $this->client->add("favorite_number", 17, $records);
        $this->assertEquals($records, $this->client->inventory());
    }

    public function testJsonifyRecords(){
        $record1 = 1;
        $record2 = 2;
        $data = [
            "int" => 1,
            "multi" => [1, 2, 3, 4]
        ];
        $this->client->insert(['data' => $data, 'records' => [$record1, $record2]]);
        $dump = $this->client->jsonify(['records' => [$record1, $record2]]);
        $expected = [
            "int" => [1],
            "multi" => [1, 2, 3, 4]
        ];
        $this->assertEquals([$expected, $expected], json_decode($dump, true));
    }

    public function testJsonifyRecordsIdentifier(){
        $record1 = 1;
        $record2 = 2;
        $data = [
            "int" => 1,
            "multi" => [1, 2, 3, 4]
        ];
        $this->client->insert(['data' => $data, 'records' => [$record1, $record2]]);
        $dump = $this->client->jsonify(['records' => [$record1, $record2], 'includeId' => true]);
        $expected1 = [
            "int" => [1],
            "multi" => [1, 2, 3, 4],
            Concourse\Thrift\Constant::get('JSON_RESERVED_IDENTIFIER_NAME') => 1
        ];
        $expected2 = [
            "int" => [1],
            "multi" => [1, 2, 3, 4],
            Concourse\Thrift\Constant::get('JSON_RESERVED_IDENTIFIER_NAME') => 2
        ];
        $this->assertEquals([$expected1, $expected2], json_decode($dump, true));
    }

    public function testJsonifyRecordsTime(){
        $record1 = 1;
        $record2 = 2;
        $data = [
            "int" => 1,
            "multi" => [1, 2, 3, 4]
        ];
        $this->client->insert(['data' => $data, 'records' => [$record1, $record2]]);
        $time = $this->client->time();
        $this->client->add("foo", 10, [$record1, $record2]);
        $dump = $this->client->jsonify(['records' => [$record1, $record2], 'time' => $time]);
        $expected = [
            "int" => [1],
            "multi" => [1, 2, 3, 4]
        ];
        $this->assertEquals([$expected, $expected], json_decode($dump, true));
    }

    public function testJsonifyRecordsTimestr(){
        $record1 = 1;
        $record2 = 2;
        $data = [
            "int" => 1,
            "multi" => [1, 2, 3, 4]
        ];
        $this->client->insert(['data' => $data, 'records' => [$record1, $record2]]);
        $anchor = $this->getTimeAnchor();
        $this->client->add("foo", 10, [$record1, $record2]);
        $time = $this->getElapsedMillisString($anchor);
        $dump = $this->client->jsonify(['records' => [$record1, $record2], 'time' => $time]);
        $expected = [
            "int" => [1],
            "multi" => [1, 2, 3, 4]
        ];
        $this->assertEquals([$expected, $expected], json_decode($dump, true));
    }

    public function testJsonifyRecordsIdentifierTime(){
        $record1 = 1;
        $record2 = 2;
        $data = [
            "int" => 1,
            "multi" => [1, 2, 3, 4]
        ];
        $this->client->insert(['data' => $data, 'records' => [$record1, $record2]]);
        $time = $this->client->time();
        $this->client->add("foo", 10, [$record1, $record2]);
        $dump = $this->client->jsonify(['records' => [$record1, $record2], 'time' => $time, 'includeId' => true]);
        $expected1 = [
            "int" => [1],
            "multi" => [1, 2, 3, 4],
            Concourse\Thrift\Constant::get('JSON_RESERVED_IDENTIFIER_NAME') => 1
        ];
        $expected2 = [
            "int" => [1],
            "multi" => [1, 2, 3, 4],
            Concourse\Thrift\Constant::get('JSON_RESERVED_IDENTIFIER_NAME') => 2
        ];
        $this->assertEquals([$expected1, $expected2], json_decode($dump, true));
    }

    public function testJsonifyRecordsIdentifierTimestr(){
        $this->markTestSkipped("CON-332: This test is flaky for reasons unknown at this time");
        $record1 = 1;
        $record2 = 2;
        $data = [
            "int" => 1,
            "multi" => [1, 2, 3, 4]
        ];
        $this->client->insert(['data' => $data, 'records' => [$record1, $record2]]);
        $anchor = $this->getTimeAnchor();
        $this->client->add("foo", 10, [$record1, $record2]);
        $time = $this->getElapsedMillisString($anchor);
        $dump = $this->client->jsonify(['records' => [$record1, $record2], 'time' => $time, 'includeId' => true]);
        $expected1 = [
            "int" => [1],
            "multi" => [1, 2, 3, 4],
            Concourse\Thrift\Constant::get('JSON_RESERVED_IDENTIFIER_NAME') => 1
        ];
        $expected2 = [
            "int" => [1],
            "multi" => [1, 2, 3, 4],
            Concourse\Thrift\Constant::get('JSON_RESERVED_IDENTIFIER_NAME') => 2
        ];
        $this->assertEquals([$expected1, $expected2], json_decode($dump, true));
    }

    public function testLinkKeySourceDestination(){
        $this->assertTrue($this->client->link("friends", 1, 2));
        $this->assertEquals(Concourse\Link::to(2), $this->client->get("friends", ['records' => 1]));
    }

    public function testLinkKeySourceDestinations(){
        $this->client->link("friends", 1, 5);
        $this->assertEquals([
            1 => true,
            2 => true,
            3 => true,
            4 => true,
            5 => false
        ], $this->client->link("friends", 1, [1, 2, 3, 4, 5]));
    }

    public function testPing(){
        $record = 1;
        $this->assertTrue(!$this->client->ping($record));
        $this->client->add("foo", 1, $record);
        $this->assertTrue($this->client->ping($record));
        $this->client->clear("foo", $record);
        $this->assertTrue(!$this->client->ping($record));
    }

    public function testPingRecords(){
        $this->client->add("foo", 1, [1, 2]);
        $data = $this->client->ping([1, 2, 3]);
        $expected = [
            1 => true,
            2 => true,
            3 => false
        ];
        $this->assertEquals($expected, $data);
    }

    public function testRemoveKeyValueRecord(){
        $key = "foo";
        $value = 1;
        $record = 1;
        $this->assertTrue(!$this->client->remove($key, $value, $record));
        $this->client->add($key, $value, $record);
        $this->assertTrue($this->client->remove($key, $value, $record));
    }

    public function testRemoveKeyValueRecords(){
        $key = "foo";
        $value = 1;
        $this->client->add($key, $value, [1, 2]);
        $this->assertEquals([1 => true, 2 => true, 3 => false], $this->client->remove($key, $value, [1, 2, 3]));
    }

    public function testRevertKeyRecordsTime(){
        $data1 = [
            'one' => 1,
            'two' => 2,
            'three' => 3
        ];
        $data2 = [
            'one' => true,
            'two' => true,
            'three' => true
        ];
        $this->client->insert(['data' => $data1, 'records' => [1, 2, 3]]);
        $time = $this->client->time();
        $this->client->insert(['data' => $data2, 'records' => [1, 2, 3]]);
        $this->client->revert("one", [1, 2, 3], $time);
        $data = $this->client->select(['key' => 'one', 'records' => [1, 2, 3]]);
        $this->assertEquals([
            1 => [1],
            2 => [1],
            3 => [1]
            ], $data);
    }

    public function testRevertKeyRecordsTimestr(){
        $data1 = [
            'one' => 1,
            'two' => 2,
            'three' => 3
        ];
        $data2 = [
            'one' => true,
            'two' => true,
            'three' => true
        ];
        $this->client->insert(['data' => $data1, 'records' => [1, 2, 3]]);
        $anchor = $this->getTimeAnchor();
        $this->client->insert(['data' => $data2, 'records' => [1, 2, 3]]);
        $time = $this->getElapsedMillisString($anchor);
        $this->client->revert("one", [1, 2, 3], $time);
        $data = $this->client->select(['key' => 'one', 'records' => [1, 2, 3]]);
        $this->assertEquals([
            1 => [1],
            2 => [1],
            3 => [1]
            ], $data);
    }

    public function testRevertKeysRecordsTime(){
        $data1 = [
            'one' => 1,
            'two' => 2,
            'three' => 3
        ];
        $data2 = [
            'one' => true,
            'two' => true,
            'three' => true
        ];
        $this->client->insert(['data' => $data1, 'records' => [1, 2, 3]]);
        $time = $this->client->time();
        $this->client->insert(['data' => $data2, 'records' => [1, 2, 3]]);
        $this->client->revert(["one", "two", "three"], [1, 2, 3], $time);
        $data = $this->client->select(['key' => ['one', 'two', 'three'], 'records' => [1, 2, 3]]);
        $data3 = [
            'one' => [1],
            'two' => [2],
            'three' => [3]
        ];
        $this->assertEquals([
            1 => $data3,
            2 => $data3,
            3 => $data3
            ], $data);
    }

    public function testRevertKeysRecordsTimestr(){
        $data1 = [
            'one' => 1,
            'two' => 2,
            'three' => 3
        ];
        $data2 = [
            'one' => true,
            'two' => true,
            'three' => true
        ];
        $this->client->insert(['data' => $data1, 'records' => [1, 2, 3]]);
        $anchor = $this->getTimeAnchor();
        $this->client->insert(['data' => $data2, 'records' => [1, 2, 3]]);
        $time = $this->getElapsedMillisString($anchor);
        $this->client->revert(["one", "two", "three"], [1, 2, 3], $time);
        $data = $this->client->select(['key' => ['one', 'two', 'three'], 'records' => [1, 2, 3]]);
        $data3 = [
            'one' => [1],
            'two' => [2],
            'three' => [3]
        ];
        $this->assertEquals([
            1 => $data3,
            2 => $data3,
            3 => $data3
            ], $data);
    }

    public function testRevertKeysRecordTime(){
        $data1 = [
            'one' => 1,
            'two' => 2,
            'three' => 3
        ];
        $data2 = [
            'one' => true,
            'two' => true,
            'three' => true
        ];
        $this->client->insert(['data' => $data1, 'records' => [1, 2, 3]]);
        $time = $this->client->time();
        $this->client->insert(['data' => $data2, 'records' => [1, 2, 3]]);
        $this->client->revert(["one", "two", "three"], [1], $time);
        $data = $this->client->select(['key' => ['one', 'two', 'three'], 'records' => 1]);
        $data3 = [
            'one' => [1],
            'two' => [2],
            'three' => [3]
        ];
        $this->assertEquals($data3, $data);
    }

    public function testRevertKeysRecordTimestr(){
        $data1 = [
            'one' => 1,
            'two' => 2,
            'three' => 3
        ];
        $data2 = [
            'one' => true,
            'two' => true,
            'three' => true
        ];
        $this->client->insert(['data' => $data1, 'records' => [1, 2, 3]]);
        $anchor = $this->getTimeAnchor();
        $this->client->insert(['data' => $data2, 'records' => [1, 2, 3]]);
        $time = $this->getElapsedMillisString($anchor);
        $this->client->revert(["one", "two", "three"], [1], $time);
        $data = $this->client->select(['key' => ['one', 'two', 'three'], 'records' => 1]);
        $data3 = [
            'one' => [1],
            'two' => [2],
            'three' => [3]
        ];
        $this->assertEquals($data3, $data);
    }

    public function testRevertKeyRecordTime(){
        $data1 = [
            'one' => 1,
            'two' => 2,
            'three' => 3
        ];
        $data2 = [
            'one' => true,
            'two' => true,
            'three' => true
        ];
        $this->client->insert(['data' => $data1, 'records' => [1, 2, 3]]);
        $time = $this->client->time();
        $this->client->insert(['data' => $data2, 'records' => [1, 2, 3]]);
        $this->client->revert(["one"], [1], $time);
        $data = $this->client->select(['key' => 'one', 'records' => 1]);
        $this->assertEquals([1], $data);
    }

    public function testRevertKeyRecordTimestr(){
        $data1 = [
            'one' => 1,
            'two' => 2,
            'three' => 3
        ];
        $data2 = [
            'one' => true,
            'two' => true,
            'three' => true
        ];
        $this->client->insert(['data' => $data1, 'records' => [1, 2, 3]]);
        $anchor = $this->getTimeAnchor();
        $this->client->insert(['data' => $data2, 'records' => [1, 2, 3]]);
        $time = $this->getElapsedMillisString($anchor);
        $this->client->revert(["one"], [1], $time);
        $data = $this->client->select(['key' => 'one', 'records' => 1]);
        $this->assertEquals([1], $data);
    }

    public function testSearch(){
        $this->client->add("name", "jeff", 1);
        $this->client->add("name", "jeffery", 2);
        $this->client->add("name", "jeremy", 3);
        $this->client->add("name", "ben jefferson", 4);
        $records = $this->client->search("name", "jeff");
        $this->assertEquals([1, 2, 4], $records);
    }

    public function testSelectCcl(){
        $key1 = random_string();
        $key2 = random_string();
        $record1 = rand();
        $record2 = rand();
        $this->client->add($key1, 1, [$record1, $record2]);
        $this->client->add($key1, 2, [$record1, $record2]);
        $this->client->add($key1, 3, [$record1, $record2]);
        $this->client->add($key2, 10, [$record1, $record2]);
        $ccl = "$key2 = 10";
        $data = $this->client->select(['ccl' => $ccl]);
        $expected = [
            $key1 => [1, 2, 3],
            $key2 => [10]
        ];
        $this->assertEquals($expected, $data[$record1]);
        $this->assertEquals($expected, $data[$record2]);
    }

    public function testSelectCclTime(){
        $key1 = random_string();
        $key2 = random_string();
        $record1 = rand();
        $record2 = rand();
        $this->client->add($key1, 1, [$record1, $record2]);
        $this->client->add($key1, 2, [$record1, $record2]);
        $this->client->add($key1, 3, [$record1, $record2]);
        $this->client->add($key2, 10, [$record1, $record2]);
        $time = $this->client->time();
        $this->client->set($key2, 11, [$record1, $record2]);
        $ccl = "$key2 = 10";
        $data = $this->client->select(['ccl' => $ccl, 'time' => $time]);
        $expected = [
            $key1 => [1, 2, 3],
            $key2 => [10]
        ];
        $this->assertEquals($expected, $data[$record1]);
        $this->assertEquals($expected, $data[$record2]);
    }

    public function testSelectCclTimestr(){
        $key1 = random_string();
        $key2 = random_string();
        $record1 = rand();
        $record2 = rand();
        $this->client->add($key1, 1, [$record1, $record2]);
        $this->client->add($key1, 2, [$record1, $record2]);
        $this->client->add($key1, 3, [$record1, $record2]);
        $this->client->add($key2, 10, [$record1, $record2]);
        $anchor = $this->getTimeAnchor();
        $this->client->set($key2, 11, [$record1, $record2]);
        $ccl = "$key2 = 10";
        $time = $this->getElapsedMillisString($anchor);
        $data = $this->client->select(['ccl' => $ccl, 'time' => $time]);
        $expected = [
            $key1 => [1, 2, 3],
            $key2 => [10]
        ];
        $this->assertEquals($expected, $data[$record1]);
        $this->assertEquals($expected, $data[$record2]);
    }

    public function testSelectKeyCcl(){
        $key1 = random_string();
        $key2 = random_string();
        $record1 = rand();
        $record2 = rand();
        $this->client->add($key1, 1, [$record1, $record2]);
        $this->client->add($key1, 2, [$record1, $record2]);
        $this->client->add($key1, 3, [$record1, $record2]);
        $this->client->add($key2, 10, [$record1, $record2]);
        $this->client->add($key1, 4, $record2);
        $ccl = "$key2 = 10";
        $data = $this->client->select(['ccl' => $ccl, 'key' => $key1]);
        $expected = [
            $record1 => [1, 2, 3],
            $record2 => [1, 2, 3, 4]
        ];
        $this->assertEquals($expected, $data);
    }

    public function testSelectKeyCclTime(){
        $key1 = random_string();
        $key2 = random_string();
        $record1 = rand();
        $record2 = rand();
        $this->client->add($key1, 1, [$record1, $record2]);
        $this->client->add($key1, 2, [$record1, $record2]);
        $this->client->add($key1, 3, [$record1, $record2]);
        $this->client->add($key2, 10, [$record1, $record2]);
        $this->client->add($key1, 4, $record2);
        $time = $this->client->time();
        $ccl = "$key2 = 10";
        $this->client->set($key1, 100, [$record2, $record1]);
        $data = $this->client->select(['ccl' => $ccl, 'key' => $key1, 'time' => $time]);
        $expected = [
            $record1 => [1, 2, 3],
            $record2 => [1, 2, 3, 4]
        ];
        $this->assertEquals($expected, $data);
    }

    public function testSelectKeyCclTimestr(){
        $key1 = random_string();
        $key2 = random_string();
        $record1 = rand();
        $record2 = rand();
        $this->client->add($key1, 1, [$record1, $record2]);
        $this->client->add($key1, 2, [$record1, $record2]);
        $this->client->add($key1, 3, [$record1, $record2]);
        $this->client->add($key2, 10, [$record1, $record2]);
        $this->client->add($key1, 4, $record2);
        $anchor = $this->getTimeAnchor();
        $ccl = "$key2 = 10";
        $this->client->set($key1, 100, [$record2, $record1]);
        $time = $this->getElapsedMillisString($anchor);
        $data = $this->client->select(['ccl' => $ccl, 'key' => $key1, 'time' => $time]);
        $expected = [
            $record1 => [1, 2, 3],
            $record2 => [1, 2, 3, 4]
        ];
        $this->assertEquals($expected, $data);
    }

    public function testSelectKeysCcl(){
        $key1 = random_string();
        $key2 = random_string();
        $record1 = rand();
        $record2 = rand();
        $this->client->add($key1, 1, [$record1, $record2]);
        $this->client->add($key1, 2, [$record1, $record2]);
        $this->client->add($key1, 3, [$record1, $record2]);
        $this->client->add($key2, 10, [$record1, $record2]);
        $this->client->add($key1, 4, $record2);
        $ccl = "$key2 = 10";
        $data = $this->client->select(['ccl' => $ccl, 'key' => [$key1, $key2]]);
        $expected = [
            $record1 => [$key1 => [1, 2, 3], $key2 => [10]],
            $record2 => [$key1 => [1, 2, 3, 4], $key2 => [10]]
        ];
        $this->assertEquals($expected, $data);
    }

    public function testSelectKeysCclTime(){
        $key1 = random_string();
        $key2 = random_string();
        $record1 = rand();
        $record2 = rand();
        $this->client->add($key1, 1, [$record1, $record2]);
        $this->client->add($key1, 2, [$record1, $record2]);
        $this->client->add($key1, 3, [$record1, $record2]);
        $this->client->add($key2, 10, [$record1, $record2]);
        $this->client->add($key1, 4, $record2);
        $time = $this->client->time();
        $ccl = "$key2 = 10";
        $this->client->set($key1, 100, [$record2, $record1]);
        $data = $this->client->select(['ccl' => $ccl, 'key' => [$key1, $key2], 'time' => $time]);
        $expected = [
            $record1 => [$key1 => [1, 2, 3], $key2 => [10]],
            $record2 => [$key1 => [1, 2, 3, 4], $key2 => [10]]
        ];
        $this->assertEquals($expected, $data);
    }

    public function testSelectKeysCclTimestr(){
        $key1 = random_string();
        $key2 = random_string();
        $record1 = rand();
        $record2 = rand();
        $this->client->add($key1, 1, [$record1, $record2]);
        $this->client->add($key1, 2, [$record1, $record2]);
        $this->client->add($key1, 3, [$record1, $record2]);
        $this->client->add($key2, 10, [$record1, $record2]);
        $this->client->add($key1, 4, $record2);
        $anchor = $this->getTimeAnchor();
        $ccl = "$key2 = 10";
        $this->client->set($key1, 100, [$record2, $record1]);
        $time = $this->getElapsedMillisString($anchor);
        $data = $this->client->select(['ccl' => $ccl, 'key' => [$key1, $key2], 'time' => $time]);
        $expected = [
            $record1 => [$key1 => [1, 2, 3], $key2 => [10]],
            $record2 => [$key1 => [1, 2, 3, 4], $key2 => [10]]
        ];
        $this->assertEquals($expected, $data);
    }

    public function testSelectKeyRecord(){
        $this->client->add("foo", 1, 1);
        $this->client->add("foo", 2, 1);
        $this->client->add("foo", 3, 1);
        $this->assertEquals([1, 2, 3], $this->client->select(['key' => "foo", 'record' => 1]));
    }

    public function testSelectKeyRecordTime(){
        $this->client->add("foo", 1, 1);
        $this->client->add("foo", 2, 1);
        $this->client->add("foo", 3, 1);
        $time = $this->client->time();
        $this->client->add("foo", 4, 1);
        $this->assertEquals([1, 2, 3], $this->client->select(['key' => "foo", 'record' => 1, 'time' => $time]));
    }

    public function testSelectKeyRecordTimestr(){
        $this->client->add("foo", 1, 1);
        $this->client->add("foo", 2, 1);
        $this->client->add("foo", 3, 1);
        $anchor = $this->getTimeAnchor();
        $this->client->add("foo", 4, 1);
        $time = $this->getElapsedMillisString($anchor);
        $this->assertEquals([1, 2, 3], $this->client->select(['key' => "foo", 'record' => 1, 'time' => $time]));
    }

    public function testSelectKeyRecords(){
        $this->client->add("foo", 1, [1, 2, 3]);
        $this->client->add("foo", 2, [1, 2, 3]);
        $this->client->add("foo", 3, [1, 2, 3]);
        $expected = [
            1 => [1, 2, 3],
            2 => [1, 2, 3],
            3 => [1, 2, 3]
        ];
        $this->assertEquals($expected, $this->client->select(['key' => "foo", 'record' => [1, 2, 3]]));
    }

    public function testSelectKeyRecordsTime(){
        $this->client->add("foo", 1, [1, 2, 3]);
        $this->client->add("foo", 2, [1, 2, 3]);
        $this->client->add("foo", 3, [1, 2, 3]);
        $time = $this->client->time();
        $this->client->add("foo", 4, [1, 2, 3]);
        $expected = [
            1 => [1, 2, 3],
            2 => [1, 2, 3],
            3 => [1, 2, 3]
        ];
        $this->assertEquals($expected, $this->client->select(['key' => "foo", 'record' => [1, 2, 3], 'time' => $time]));
    }

    public function testSelectKeyRecordsTimestr(){
        $this->client->add("foo", 1, [1, 2, 3]);
        $this->client->add("foo", 2, [1, 2, 3]);
        $this->client->add("foo", 3, [1, 2, 3]);
        $anchor = $this->getTimeAnchor();
        $this->client->add("foo", 4, [1, 2, 3]);
        $time = $this->getElapsedMillisString($anchor);
        $expected = [
            1 => [1, 2, 3],
            2 => [1, 2, 3],
            3 => [1, 2, 3]
        ];
        $this->assertEquals($expected, $this->client->select(['key' => "foo", 'record' => [1, 2, 3], 'time' => $time]));
    }

    public function testSelectKeysRecord(){
        $this->client->add("foo", 1, 1);
        $this->client->add("foo", 2, 1);
        $this->client->add("bar", 1, 1);
        $this->client->add("bar", 2, 1);
        $data = $this->client->select(['keys' => ['foo', 'bar'], 'record' => 1]);
        $expected = [
            'foo' => [1, 2],
            'bar' => [1, 2]
        ];
        $this->assertEquals($expected, $data);
    }

    public function testSelectKeysRecordTime(){
        $this->client->add("foo", 1, 1);
        $this->client->add("foo", 2, 1);
        $this->client->add("bar", 1, 1);
        $this->client->add("bar", 2, 1);
        $time = $this->client->time();
        $this->client->add("foo", 3, 1);
        $this->client->add("bar", 3, 1);
        $data = $this->client->select(['keys' => ['foo', 'bar'], 'record' => 1, 'time' => $time]);
        $expected = [
            'foo' => [1, 2],
            'bar' => [1, 2]
        ];
        $this->assertEquals($expected, $data);
    }

    public function testSelectKeysRecordTimestr(){
        $this->client->add("foo", 1, 1);
        $this->client->add("foo", 2, 1);
        $this->client->add("bar", 1, 1);
        $this->client->add("bar", 2, 1);
        $anchor = $this->getTimeAnchor();
        $this->client->add("foo", 3, 1);
        $this->client->add("bar", 3, 1);
        $time = $this->getElapsedMillisString($anchor);
        $data = $this->client->select(['keys' => ['foo', 'bar'], 'record' => 1, 'time' => $time]);
        $expected = [
            'foo' => [1, 2],
            'bar' => [1, 2]
        ];
        $this->assertEquals($expected, $data);
    }

    public function testSelectKeysRecords(){
        $this->client->add("foo", 1, [1, 2]);
        $this->client->add("foo", 2, [1, 2]);
        $this->client->add("bar", 1, [1, 2]);
        $this->client->add("bar", 2, [1, 2]);
        $data = $this->client->select(['keys' => ['foo', 'bar'], 'record' => [1, 2]]);
        $expected = [
            'foo' => [1, 2],
            'bar' => [1, 2]
        ];
        $this->assertEquals([
            1 => $expected,
            2 => $expected], $data);
    }

    public function testSelectKeysRecordsTime(){
        $this->client->add("foo", 1, [1, 2]);
        $this->client->add("foo", 2, [1, 2]);
        $this->client->add("bar", 1, [1, 2]);
        $this->client->add("bar", 2, [1, 2]);
        $time = $this->client->time();
        $this->client->add("foo", 3, [1, 2]);
        $this->client->add("bar", 3, [1, 2]);
        $data = $this->client->select(['keys' => ['foo', 'bar'], 'record' => [1, 2], 'time' => $time]);
        $expected = [
            'foo' => [1, 2],
            'bar' => [1, 2]
        ];
        $this->assertEquals([
            1 => $expected,
            2 => $expected], $data);
    }

    public function testSelectKeysRecordsTimestr(){
        $this->client->add("foo", 1, [1, 2]);
        $this->client->add("foo", 2, [1, 2]);
        $this->client->add("bar", 1, [1, 2]);
        $this->client->add("bar", 2, [1, 2]);
        $anchor = $this->getTimeAnchor();
        $this->client->add("foo", 3, [1, 2]);
        $this->client->add("bar", 3, [1, 2]);
        $time = $this->getElapsedMillisString($anchor);
        $data = $this->client->select(['keys' => ['foo', 'bar'], 'record' => [1, 2], 'time' => $time]);
        $expected = [
            'foo' => [1, 2],
            'bar' => [1, 2]
        ];
        $this->assertEquals([
            1 => $expected,
            2 => $expected], $data);
    }

    public function testSelectRecord(){
        $this->client->add("foo", 1, [1, 2]);
        $this->client->add("foo", 2, [1, 2]);
        $this->client->add("bar", 1, [1, 2]);
        $this->client->add("bar", 2, [1, 2]);
        $data = $this->client->select(1);
        $expected = [
            'foo' => [1, 2],
            'bar' => [1, 2]
        ];
        $this->assertEquals($expected, $data);
    }

    public function testSelectRecordTime(){
        $this->client->add("foo", 1, [1, 2]);
        $this->client->add("foo", 2, [1, 2]);
        $this->client->add("bar", 1, [1, 2]);
        $this->client->add("bar", 2, [1, 2]);
        $time = $this->client->time();
        $this->client->add("foo", 3, [1, 2]);
        $this->client->add("bar", 3, [1, 2]);
        $data = $this->client->select(1, $time);
        $expected = [
            'foo' => [1, 2],
            'bar' => [1, 2]
        ];
        $this->assertEquals($expected, $data);
    }

    public function testSelectRecordTimestr(){
        $this->client->add("foo", 1, [1, 2]);
        $this->client->add("foo", 2, [1, 2]);
        $this->client->add("bar", 1, [1, 2]);
        $this->client->add("bar", 2, [1, 2]);
        $anchor = $this->getTimeAnchor();
        $this->client->add("foo", 3, [1, 2]);
        $this->client->add("bar", 3, [1, 2]);
        $time = $this->getElapsedMillisString($anchor);
        $data = $this->client->select(1, $time);
        $expected = [
            'foo' => [1, 2],
            'bar' => [1, 2]
        ];
        $this->assertEquals($expected, $data);
    }

    public function testSelectRecords(){
        $this->client->add("foo", 1, [1, 2]);
        $this->client->add("foo", 2, [1, 2]);
        $this->client->add("bar", 1, [1, 2]);
        $this->client->add("bar", 2, [1, 2]);
        $data = $this->client->select([1, 2]);
        $expected = [
            'foo' => [1, 2],
            'bar' => [1, 2]
        ];
        $this->assertEquals([
                1 => $expected,
                2 => $expected
            ], $data);
    }

    public function testSelectRecordsTime(){
        $this->client->add("foo", 1, [1, 2]);
        $this->client->add("foo", 2, [1, 2]);
        $this->client->add("bar", 1, [1, 2]);
        $this->client->add("bar", 2, [1, 2]);
        $time = $this->client->time();
        $this->client->add("foo", 3, [1, 2]);
        $this->client->add("bar", 3, [1, 2]);
        $data = $this->client->select([1, 2], ['time' => $time]);
        $expected = [
            'foo' => [1, 2],
            'bar' => [1, 2]
        ];
        $this->assertEquals([
                1 => $expected,
                2 => $expected
            ], $data);
    }

    public function testSelectRecordsTimestr(){
        $this->client->add("foo", 1, [1, 2]);
        $this->client->add("foo", 2, [1, 2]);
        $this->client->add("bar", 1, [1, 2]);
        $this->client->add("bar", 2, [1, 2]);
        $anchor = $this->getTimeAnchor();
        $this->client->add("foo", 3, [1, 2]);
        $this->client->add("bar", 3, [1, 2]);
        $time = $this->getElapsedMillisString($anchor);
        $data = $this->client->select([1, 2], ['time' => $time]);
        $expected = [
            'foo' => [1, 2],
            'bar' => [1, 2]
        ];
        $this->assertEquals([
                1 => $expected,
                2 => $expected
            ], $data);
    }

    public function testSetKeyValue(){
        $key = "foo";
        $value = 1;
        $record = $this->client->set($key, $value);
        $data = $this->client->select($record);
        $this->assertEquals([
            'foo' => [1]
            ], $data);
    }

    public function testSetKeyValueRecord(){
        $this->client->add("foo", 2, 1);
        $this->client->add("foo", 3, 1);
        $this->client->set("foo", 1, 1);
        $data = $this->client->select(1);
        $this->assertEquals([
            'foo' => [1]
            ], $data);
    }

    public function testSetKeyValueRecords(){
        $this->client->add("foo", 2, [1, 2, 3]);
        $this->client->add("foo", 3, [1, 2, 3]);
        $this->client->set("foo", 1, [1, 2, 3]);
        $data = $this->client->select([1, 2, 3]);
        $expected = ['foo' => [1]];
        $this->assertEquals([
            1 => $expected,
            2 => $expected,
            3 => $expected
            ], $data);
    }

    public function testStage(){
        try {
            $ref = new ReflectionClass(get_class($this->client));
            $prop = $ref->getProperty('transaction');
            $prop->setAccessible(true);
            $token = $prop->getValue($this->client);
            $this->assertNull($token);
            $this->client->stage();
            $token = $token = $prop->getValue($this->client);
            $this->assertTrue(!is_null($token));
        }
        catch(Exception $e){
            $this->client->abort();
        }
        $this->client->abort();
    }

    public function testStageCallable(){
        $this->client->stage(function() {
            $this->client->add("name", "jeff", 17);
        });
        $this->assertEquals("jeff", $this->client->get(['key' => "name", 'record' => 17]));
    }

    public function testStageCallableTransactionException(){
        $this->setExpectedException('Concourse\Thrift\Exceptions\TransactionException');
        $this->client->stage(function() {
            $this->client->find("throw transaction exception");
        });
    }

    public function testStageCallableEmbedded(){
        $this->client->stage(function(){
            $this->client->stage();
        });
        $ref = new ReflectionClass(get_class($this->client));
        $prop = $ref->getProperty('transaction');
        $prop->setAccessible(true);
        $token = $prop->getValue($this->client);
        $this->assertTrue(is_null($token));
    }

    public function testTime(){
        $this->assertTrue(is_integer($this->client->time()));
    }

    public function testTimePhrase(){
        $this->assertTrue(is_integer($this->client->time("3 seconds ago")));
    }

    public function testVerifyAndSwap(){
        $this->client->add("foo", 2, 2);
        $this->assertTrue(!$this->client->verifyAndSwap("foo", 1, 2, 3));
        $this->assertTrue($this->client->verifyAndSwap("foo", 2, 2, 3));
        $this->assertEquals(3, $this->client->get(['key' => "foo", 'record' => 2]));
    }

    public function testVerifyOrSet(){
        $this->client->add("foo", 2, 2);
        $this->client->verifyOrSet("foo", 3, 2);
        $this->assertEquals(3, $this->client->get(['key' => "foo", 'record' => 2]));
    }

    public function testVerifyKeyValueRecord(){
        $this->client->add("name", "jeff", 1);
        $this->client->add("name", "jeffery", 1);
        $this->client->add("name", "bob", 1);
        $this->assertTrue($this->client->verify("name", "jeff", 1));
        $this->client->remove("name", "jeff", 1);
        $this->assertTrue(!$this->client->verify("name", "jeff", 1));
    }

    public function testVerifyKeyValueRecordTime(){
        $this->client->add("name", "jeff", 1);
        $this->client->add("name", "jeffery", 1);
        $this->client->add("name", "bob", 1);
        $time = $this->client->time();
        $this->client->remove("name", "jeff", 1);
        $this->assertTrue($this->client->verify("name", "jeff", 1, $time));
    }

    public function testVerifyKeyValueRecordTimestr(){
        $this->client->add("name", "jeff", 1);
        $this->client->add("name", "jeffery", 1);
        $this->client->add("name", "bob", 1);
        $anchor = $this->getTimeAnchor();
        $this->client->remove("name", "jeff", 1);
        $time = $this->getElapsedMillisString($anchor);
        $this->assertTrue($this->client->verify("name", "jeff", 1, $time));
    }

    public function testUnlinkKeySourceDestination(){
        $this->client->link("friends", 1, 2);
        $this->assertTrue($this->client->unlink("friends", 1, 2));
    }

    public function testUnlinkKeySourceDestinations(){
        $this->client->link("friends", 1, 2);
        $this->assertEquals([
            2 => true,
            3 => false
            ], $this->client->unlink("friends", 1, [2, 3]));
    }

    public function testFindOrAddKeyValue(){
        $record = $this->client->findOrAdd("age", 23);
        $this->assertEquals(23, $this->client->get("age", ['record' => $record]));
    }

    public function testFindOrInsertCclJson(){
        $data = [
            'name' => "Jeff Nelson"
        ];
        $data = json_encode($data);
        $record = $this->client->findOrInsert("age > 10", $data);
        $this->assertEquals("Jeff Nelson", $this->client->get("name", ['record' => $record]));
    }

    public function testFindOrInsertCclHash(){
        $data = [
            'name' => "Jeff Nelson"
        ];
        $record = $this->client->findOrInsert("age > 10", $data);
        $this->assertEquals("Jeff Nelson", $this->client->get("name", ['record' => $record]));
    }

    public function testReconcileEmptyValues(){
        $this->client->reconcile("foo", 17, []);
        $data = $this->client->select(["key"=>"foo", "record" => 17]);
        $this->assertTrue(empty($data));
    }

    public function testReconcile(){
        $field = "testKey";
        $record = 1;
        $this->client->add($field, "A", $record);
        $this->client->add($field, "C", $record);
        $this->client->add($field, "D", $record);
        $this->client->add($field, "E", $record);
        $this->client->add($field, "F", $record);
        $values = array("A", "B", "D", "G");
        $this->client->reconcile($field, $record, $values);
        $stored = $this->client->select(["key"=>$field, "record" => $record]);
        $this->assertEquals(count($values), count($stored));
        foreach($stored as $value){
            $this->assertTrue(in_array($value, $values));
        }
    }

    public function testInvokePlugin(){
        $count = rand(1, 20);
        $params = array();
        for($i = 0; $i < $count; ++$i){
            $params[] = $i;
        }
        $value = $this->client->invokePlugin("com.cinchapi.fake.plugin.PluginClass", "fakeMethod",  $params);
        $this->assertEquals(count($params), $value);
    }

    public function testComplexTObjectSerializeString(){
        $expected = random_string();
        $actual = ComplexTObject::fromPhpObject($expected);
        $actual = $actual->getPhpObject();
        $this->assertEquals($expected, $actual);
    }

    public function testComplexTObjectSerializeInt(){
        $expected = rand(MIN_INT, MAX_INT);
        $actual = ComplexTObject::fromPhpObject($expected);
        $actual = $actual->getPhpObject();
        $this->assertEquals($expected, $actual);
    }

    public function testComplexTObjectSerializeBoolean(){
        $expected = current_time_millis() % 2 == 0 ? true : false;
        $actual = ComplexTObject::fromPhpObject($expected);
        $actual = $actual->getPhpObject();
        $this->assertEquals($expected, $actual);
    }

    public function testComplexTObjectSerializeLong(){
        $expected = rand(MIN_INT + 1, PHP_INT_MAX);
        $actual = ComplexTObject::fromPhpObject($expected);
        $actual = $actual->getPhpObject();
        $this->assertEquals($expected, $actual);
    }

    public function testComplexTObjectSerializeArrayBasic(){
        $expected = array(1, 2, 3, 4, 5, 6, 7, 8, "9");
        $actual = ComplexTObject::fromPhpObject($expected);
        $actual = $actual->getPhpObject();
        $this->assertEquals($expected, $actual);
    }

    public function testComplexTObjectSerializeArray(){
        $count =
        $expected = array(1, 2, 3, 4, 5, 6, 7, 8, "9");
        $actual = ComplexTObject::fromPhpObject($expected);
        $actual = $actual->getPhpObject();
        $this->assertEquals($expected, $actual);
    }

}
