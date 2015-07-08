<?php
require_once dirname(__FILE__) . "/IntegrationBaseTest.php";

use Thrift\Shared\Type;

/*
 * Copyright 2015 Cinchapi, Inc.
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
}
