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

use Cinchapi\Concourse\Core as core;
use Thrift\Shared\Type;
use Thrift\Shared\Diff;

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
            $this->assertTrue(core\str_starts_with($v, $expected));
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
        $this->client->commit();
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
}
