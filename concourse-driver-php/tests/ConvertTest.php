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
require_once dirname(__FILE__) . "/test_utils.php";

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

/**
 * Unit tests for the Convert utility class.
 *
 * @author jnelson
 */
class ConvertTest extends \PHPUnit_Framework_TestCase {

    public function testConvertStringRoundTrip(){
        $obj = random_string();
        $this->assertEquals($obj, Concourse\Convert::thriftToPhp(Concourse\Convert::phpToThrift($obj)));
    }

    public function testConvertTagRoundTrip(){
        $obj = Concourse\Tag::create(random_string());
        $this->assertEquals($obj, Concourse\Convert::thriftToPhp(Concourse\Convert::phpToThrift($obj)));
    }

    public function testConvertIntRoundTrip(){
        $obj = 100;
        $this->assertEquals($obj, Concourse\Convert::thriftToPhp(Concourse\Convert::phpToThrift($obj)));
    }

    public function testConvertLongRoundTrip(){
        $obj = 2147483649;
        $this->assertEquals($obj, Concourse\Convert::thriftToPhp(Concourse\Convert::phpToThrift($obj)));
    }

    public function testConvertLinkRoundTrip(){
        $obj = Concourse\Link::to(2147483648);
        $this->assertEquals($obj, Concourse\Convert::thriftToPhp(Concourse\Convert::phpToThrift($obj)));
    }

    public function testConvertBooleanRoundTrip(){
        $obj = false;
        $this->assertEquals($obj, Concourse\Convert::thriftToPhp(Concourse\Convert::phpToThrift($obj)));
    }

    public function testConvertFloatRoundTrip(){
        $obj = 3.14353;
        $this->assertEquals($obj, Concourse\Convert::thriftToPhp(Concourse\Convert::phpToThrift($obj)));
    }

    public function testConvertDateTimeRoundTrip(){
        $obj = new DateTime();
        $this->assertEquals($obj, Concourse\Convert::thriftToPhp(Concourse\Convert::phpToThrift($obj)));
    }
}
