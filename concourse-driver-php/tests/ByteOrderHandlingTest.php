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
  * Unit tests that prove the driver correctly converts between BIG and LITTLE
  * ENDIAN byte order when necessary.
  *
  * @author Jeff Nelson
  */
  class ByteOrderHandlingTest extends IntegrationBaseTest {

      public function testInsertLinkByteOrder(){
          $link = Link::to(rand());
          $data = ['foo' => $link];
          $record = $this->client->insert(['data' => $data])[0];
          $stored = $this->client->get(['key' => 'foo', 'record' => $record]);
          $this->assertEquals(get_class($link), get_class($stored));
          $this->assertEquals($link->getRecord(), $stored->getRecord());
      }

      public function testLinkByteOrder(){
          $source = rand();
          $dest = -1 * $source;
          $this->client->link('foo', $source, $dest);
          $link = Link::to($dest);
          $stored = $this->client->get(['key' => 'foo', 'record' => $source]);
          $this->assertEquals(get_class($link), get_class($stored));
          $this->assertEquals($link->getRecord(), $stored->getRecord());
      }

      public function testInsertLongByteOrder() {
          $value = rand(MAX_INT+1, PHP_INT_MAX);
          $data = ['foo' => $value];
          $record = $this->client->insert(['data' => $data])[0];
          $stored = $this->client->get(['key' => 'foo', 'record' => $record]);
          $this->assertEquals(gettype($value), gettype($stored));
          $this->assertEquals($value, $stored);
      }

      public function testLongByteOrder() {
          $value = rand(MAX_INT+1, PHP_INT_MAX);
          $record = $this->client->add('foo', $value);
          $stored = $this->client->get(['key' => 'foo', 'record' => $record]);
          $this->assertEquals(gettype($value), gettype($stored));
          $this->assertEquals($value, $stored);
      }

      public function testInsertIntegerByteOrder(){
          $value = rand(MIN_INT, MAX_INT);
          $data = ['foo' => $value];
          $record = $this->client->insert(['data' => $data])[0];
          $stored = $this->client->get(['key' => 'foo', 'record' => $record]);
          $this->assertEquals(gettype($value), gettype($stored));
          $this->assertEquals($value, $stored);
      }

      public function testIntegerByteOrder(){
          $value = rand(MIN_INT, MAX_INT);
          $record = $this->client->add('foo', $value);
          $stored = $this->client->get(['key' => 'foo', 'record' => $record]);
          $this->assertEquals(gettype($value), gettype($stored));
          $this->assertEquals($value, $stored);
      }

  }
