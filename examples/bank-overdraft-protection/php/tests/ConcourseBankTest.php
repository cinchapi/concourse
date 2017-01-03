<?php
/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2013-2017 Cinchapi Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

namespace Cinchapi\Examples\Bank;

require_once dirname(__FILE__) . "/../vendor/autoload.php";
require_once dirname(__FILE__) . "/core.php";

/**
 * Unit tests for the project
 *
 * @author Jeff Nelson
 */
class ConcourseBankTest extends \PHPUnit_Framework_TestCase {

    var $client = null;

    public static function setUpBeforeClass() {
        start_mockcourse();
        require_directory(dirname(__FILE__)."/../src");
    }

    public static function tearDownAfterClass() {
        stop_mockcourse();
    }


    public function testWithdrawal(){
        $cust = new ConcourseCustomer("Jeff", "Nelson");
        $acct = new ConcourseAccount(100.00, [$cust]);
        $this->assertTrue($acct->withdraw(40.38));
        $this->assertEquals(59.62, $acct->getBalance());
    }

    public function testDeposit(){
        $cust = new ConcourseCustomer("Jeff", "Nelson");
        $acct = new ConcourseAccount(100.00, [$cust]);
        $acct->deposit(100.00);
        $this->assertEquals(200.00, $acct->getBalance());
    }

    public function testOwnersAreLinks(){
        $a = new ConcourseCustomer("Jeff", "Nelson");
        $b = new ConcourseCustomer("Ashleah", "Nelson");
        $c = new ConcourseCustomer("John", "Doe");
        $acct = new ConcourseAccount(250.15, [$a, $b, $c]);
        $con = Databases::concourse();
        $links = $con->select(['key' => 'owners', 'id' => $acct->getId()]);
        $this->assertTrue(in_array(\Concourse\Link::to($a->getId()), $links));
        $this->assertTrue(in_array(\Concourse\Link::to($b->getId()), $links));
        $this->assertTrue(in_array(\Concourse\Link::to($c->getId()), $links));
    }

}
