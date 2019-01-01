/*
 * Copyright (c) 2013-2019 Cinchapi Inc.
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
package com.cinchapi.concourse.lang;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.ccl.Parser;
import com.cinchapi.ccl.Parsing;
import com.cinchapi.ccl.grammar.Expression;
import com.cinchapi.ccl.grammar.KeySymbol;
import com.cinchapi.ccl.grammar.Symbol;
import com.cinchapi.concourse.ParseException;
import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.util.Parsers;

/**
 * Unit tests for the {@link com.cinchapi.concourse.lang.Criteria} building
 * functionality.
 *
 * @author Jeff Nelson
 */
public class CriteriaTest {

    @Test(expected = IllegalStateException.class)
    public void testCannotAddSymbolToBuiltCriteria() {
        Criteria criteria = Criteria.where().key("foo")
                .operator(Operator.EQUALS).value("bar").build();
        criteria.add(new KeySymbol("baz"));
    }

    @Test
    public void testNotNeccessaryToBuildSubCriteriaInGroup() {
        Criteria.where().key("foo").operator(Operator.EQUALS).value("bar").or()
                .group(Criteria.where().key("baz")
                        .operator(Operator.GREATER_THAN).value(0).and()
                        .key("name").operator(Operator.NOT_EQUALS)
                        .value("John Doe"))
                .build();
    }

    @Test
    public void testTimestampPinning() {
        Criteria criteria = Criteria.where().key("name")
                .operator(Operator.EQUALS).value("Jeff Nelson").and()
                .group(Criteria.where().key("company").operator(Operator.EQUALS)
                        .value("Cinchapi").or().key("company")
                        .operator(Operator.EQUALS).value("Blavity"))
                .build();
        Timestamp timestamp = Timestamp.now();
        criteria = criteria.at(timestamp);
        List<Symbol> symbols = Parsing.groupExpressions(criteria.getSymbols());
        symbols.forEach((symbol) -> {
            if(symbol instanceof Expression) {
                Expression expression = (Expression) symbol;
                Assert.assertEquals(expression.raw().timestamp(),
                        timestamp.getMicros());
            }
        });
    }

    @Test
    public void testTimestampPinningSomeTimestamps() {
        Criteria criteria = Criteria.where().key("name")
                .operator(Operator.EQUALS).value("Jeff Nelson").and()
                .group(Criteria.where().key("company").operator(Operator.EQUALS)
                        .value("Cinchapi").at(Timestamp.now()).or()
                        .key("company").operator(Operator.EQUALS)
                        .value("Blavity"))
                .build();
        Timestamp timestamp = Timestamp.now();
        criteria = criteria.at(timestamp);
        List<Symbol> symbols = Parsing.groupExpressions(criteria.getSymbols());
        symbols.forEach((symbol) -> {
            if(symbol instanceof Expression) {
                Expression expression = (Expression) symbol;
                Assert.assertEquals(expression.raw().timestamp(),
                        timestamp.getMicros());
            }
        });
    }

    @Test
    public void testParseCcl() {
        String ccl = "name = jeff AND (company = Cinchapi at 12345 or company = Blavity)";
        Criteria criteria = Criteria.parse(ccl);
        Parser parser1 = Parsers.create(ccl);
        Parser parser2 = Parsers.create(criteria.getCclString());
        Assert.assertEquals(Parsing.groupExpressions(parser1.tokenize()),
                Parsing.groupExpressions(parser2.tokenize()));
    }

    @Test(expected = ParseException.class)
    public void testParseCclInvalid() {
        String ccl = "name = jeff AND (company ? Cinchapi at 12345 or company = Blavity) ";
        Criteria criteria = Criteria.parse(ccl);
        System.out.println(criteria);
    }

}
