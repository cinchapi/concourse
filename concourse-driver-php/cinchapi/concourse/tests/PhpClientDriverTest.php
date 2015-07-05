<?php
require_once dirname(__FILE__) . "/IntegrationBaseTest.php";

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
    
    public function testAddKeyValue(){
        $key = "foo";
        $value = "static value";
        $record = $this->client->add(['key' => $key, 'value' => $value]);
        $this->assertNotEmpty($record);
        $stored = $this->client->get(['key' => $key, 'record' => $record]);
        $this->assertEquals($value, $stored);
    }
}
