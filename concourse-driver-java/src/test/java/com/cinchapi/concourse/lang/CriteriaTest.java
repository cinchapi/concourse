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
package com.cinchapi.concourse.lang;

import org.junit.Test;

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.KeySymbol;
import com.cinchapi.concourse.thrift.Operator;

/**
 * Unit tests for the {@link Criteria} building functionality.
 * 
 * @author Jeff Nelson
 */
public class CriteriaTest {

    @Test(expected = IllegalStateException.class)
    public void testCannotAddSymbolToBuiltCriteria() {
        Criteria criteria = Criteria.where().key("foo")
                .operator(Operator.EQUALS).value("bar").build();
        criteria.add(KeySymbol.create("baz"));
    }

    @Test
    public void testNotNeccessaryToBuildSubCriteriaInGroup() {
        Criteria.where()
                .key("foo")
                .operator(Operator.EQUALS)
                .value("bar")
                .or()
                .group(Criteria.where().key("baz")
                        .operator(Operator.GREATER_THAN).value(0).and()
                        .key("name").operator(Operator.NOT_EQUALS)
                        .value("John Doe")).build();
    }

}
