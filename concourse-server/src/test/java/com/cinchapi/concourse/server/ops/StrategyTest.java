/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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

import org.junit.Test;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.Language;
import com.cinchapi.concourse.server.ConcourseServer;
import com.cinchapi.concourse.server.ops.Strategy.Source;
import com.cinchapi.concourse.server.storage.temp.Queue;
import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.thrift.TCriteria;
import com.cinchapi.concourse.thrift.TransactionToken;
import com.google.common.collect.ImmutableList;

/**
 * Unit tests for the {@link Strategy} framework.
 *
 * @author Jeff Nelson
 */
public class StrategyTest {

    @Test
    public void testNavigationConditionKeyUseIndexForLookup() {
        // TODO: this also requires fixes to Command to include intermediate
        // navigation stops within conditionKeys
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
        Strategy strategy = new Strategy(command, new Queue(1)); // TODO: will
                                                                 // need to use
                                                                 // a different
                                                                 // store to
                                                                 // test this
        Source source = strategy.source("email", 1);
        // FIXME: This is using RECORD because Queue's memory says everything is
        // in memory
        // Assert.assertEquals(Source.INDEX, source);
    }

}
