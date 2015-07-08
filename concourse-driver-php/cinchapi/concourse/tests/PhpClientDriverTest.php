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
}
