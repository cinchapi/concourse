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

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.server.storage.Transaction;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.thrift.TransactionToken;

/**
 * Unit test to reproduce the memory leak issue described in CON-217.
 * 
 * @author Jeff Nelson
 */
public class CON217 extends ConcourseIntegrationTest {

    @Test
    public void repro() {
        client.stage();
        client.stage();
        Map<TransactionToken, Transaction> transactions = Reflection.get(
                "transactions", Reflection.get("server", this));
        Assert.assertEquals(1, transactions.size());
    }

}
