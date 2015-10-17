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

}
