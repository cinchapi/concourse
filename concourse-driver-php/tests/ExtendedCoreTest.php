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

 require_once dirname(__FILE__) . "/../src/autoload.php";
 require_once dirname(__FILE__)."/test_utils.php";

/**
 * Unit tests for the functions defiend in extended_core.php
 *
 * @author Jeff Nelson
 */
class ExtendedCoreTest extends \PHPUnit_Framework_TestCase {

    public function testCountArrayKeysIntersect(){
        $count = rand(10, 100);
        $a = [];
        $b = [];
        foreach(range(1, $count) as $number) {
            $string = random_string();
            $time = current_time_millis();
            if($time % 2 == 0) {
                $a[$string] = $number;
            }
            if($time % 3 == 0) {
                $b[$string] = $number;
            }
        }
        $this->assertEquals(count_array_keys_intersect($a, $b), count(array_intersect(array_keys($a), array_keys($b))));
    }
}
