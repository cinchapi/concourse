/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse.bugrepro;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.TransactionException;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.thrift.Operator;

/**
 * Repro of issue described in CON-171 where a phantom read is possible when
 * using transactions.
 * 
 * @author Jeff Nelson
 */
public class CON171 extends ConcourseIntegrationTest {

    @Test(expected=TransactionException.class)
    public void repro1() {
        Concourse client2 = Concourse.connect(SERVER_HOST, SERVER_PORT,
                "admin", "admin");
        client.stage();
        client.find("foo", Operator.EQUALS, "bar");
        client2.set("foo", "bar", 1);
        Assert.assertTrue(client.find("foo", Operator.EQUALS, "bar").isEmpty());
    }
    
    @Test(expected=TransactionException.class)
    public void repro2(){
        Concourse client2 = Concourse.connect(SERVER_HOST, SERVER_PORT,
                "admin", "admin");
        client.stage();
        client.find("foo", Operator.EQUALS, "bar");
        client2.add("foo", "bar", 1);
        Assert.assertTrue(client.find("foo", Operator.EQUALS, "bar").isEmpty());
    }
    
    @Test(expected=TransactionException.class)
    public void repro3(){
        Concourse client2 = Concourse.connect(SERVER_HOST, SERVER_PORT,
                "admin", "admin");
        client.stage();
        client2.add("foo", "bar", 1);
        client.find("foo", Operator.EQUALS, "bar");
        client2.remove("foo", "bar", 1);
        Assert.assertFalse(client.find("foo", Operator.EQUALS, "bar").isEmpty());
    }

}
