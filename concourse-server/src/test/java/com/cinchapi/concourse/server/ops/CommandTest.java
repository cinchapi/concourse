/*
 * Copyright (c) 2013-2022 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse.server.ops;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.Language;
import com.cinchapi.concourse.server.ConcourseServer;
import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.thrift.TCriteria;
import com.cinchapi.concourse.thrift.TransactionToken;
import com.google.common.collect.ImmutableList;

/**
 * Unit tests for {@link Command} parsing.
 *
 * @author Jeff Nelson
 */
public class CommandTest {

    @Test
    public void testNavigationConditionKeys() {
        Method method = Reflection.getMethodUnboxed(ConcourseServer.class,
                "selectKeysCriteria", List.class, TCriteria.class,
                AccessToken.class, TransactionToken.class, String.class);
        List<String> keys = ImmutableList.of("name", "credential.email");
        TCriteria criteria = Language.translateToThriftCriteria(
                Criteria.where().key("credential.email")
                        .operator(Operator.EQUALS).value("test@test.com").and()
                        .key("_").operator(Operator.EQUALS).value("User"));
        AccessToken creds = new AccessToken();
        TransactionToken transaction = new TransactionToken();
        String environment = "";
        Command command = new Command(method, keys, criteria, creds,
                transaction, environment);
        Set<String> conditionKeys = command.conditionKeys();
        Assert.assertTrue(conditionKeys.contains("credential"));
        Assert.assertTrue(conditionKeys.contains("email"));
        Assert.assertTrue(conditionKeys.contains("_"));

    }

}
