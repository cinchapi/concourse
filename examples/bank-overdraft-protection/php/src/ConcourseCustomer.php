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

require_once dirname(__FILE__)."/Databases.php";
require_once dirname(__FILE__)."/Customer.php";

class ConcourseCustomer implements Customer {

    /**
     * The id of the Concourse record that holds the data for an instance of
     * this class.
     */
     private $id;

      /**
       * Construct a new instance
       *
       * @param string $firstName the customer's first name
       * @param string $lastName  the customer's last name
       */
      public function __construct($firstName, $lastName){
        $concourse = Databases::concourse();
        $data = [];
        $data['_class'] = get_class();
        $data['first_name'] = $firstName;
        $data['last_name'] = $lastName;
        $this->id = $concourse->insert(['data' => $data])[0];
      }

      /**
       * @Override
       */
      public function getId(){
        return $this->id;
      }
}
