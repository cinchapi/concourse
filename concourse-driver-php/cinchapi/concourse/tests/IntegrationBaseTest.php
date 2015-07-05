<?php

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
 * Base class for unit tests that use Mockcourse.
 *
 * @author jnelson
 */
abstract class IntegrationBaseTest extends PHPUnit_Framework_TestCase{
    
    static $process;
    
    public static function setUpBeforeClass() {
        parent::setUpBeforeClass();
        $script = dirname(__FILE__)."/../../../../mockcourse/mockcourse";
        static::$process = popen("bash ".$script." &", "r");
    }
    
    public static function tearDownAfterClass() {
        parent::tearDownAfterClass();
        pclose(static::$process);
    }
    
    
}
