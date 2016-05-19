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

 use Concourse\Link;

/**
 * Unit tests that validate links and resolvable links are properly inserted.
 *
 * @author Jeff Nelson
 */
 class InsertLinkTest extends IntegrationBaseTest {

     public function testInsertArrayWithLink(){
         $data = [
            'foo' => Link::to(1)
         ];
         $record = $this->client->insert(['data' => $data])[0];
         $this->assertEquals(Link::to(1), $this->client->get(['key' => 'foo', 'record' => $record]));
     }

     public function testInsertArrayWithResolvableLink(){
         $record1 = $this->client->add('foo', 1);
         $record2 = $this->client->insert([
             'data' => ['foo' => Link::toWhere('foo = 1')]
         ])[0];
         $this->assertEquals(Link::to($record1), $this->client->get(['key' => 'foo', 'record' => $record2]));
     }

 }
