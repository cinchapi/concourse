/*
 * Copyright (c) 2013-2018 Cinchapi Inc.
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
import java.util.Queue;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.ccl.JavaCCParser;
import com.cinchapi.ccl.Parser;
import com.cinchapi.ccl.Parsing;
import com.cinchapi.ccl.SyntaxException;
import com.cinchapi.ccl.grammar.ConjunctionSymbol;
import com.cinchapi.ccl.grammar.Expression;
import com.cinchapi.ccl.grammar.KeySymbol;
import com.cinchapi.ccl.grammar.OperatorSymbol;
import com.cinchapi.ccl.grammar.ParenthesisSymbol;
import com.cinchapi.ccl.grammar.PostfixNotationSymbol;
import com.cinchapi.ccl.grammar.Symbol;
import com.cinchapi.ccl.grammar.ValueSymbol;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Parsers;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

/**
 * Unit tests for the {@link com.cinchapi.concourse.lang.Parser}.
 *
 * @author Jeff Nelson
 */
public class ParserTest {

    @Test
    public void testGroupSingle() {
        String key = TestData.getString();
        Operator operator = Operator.EQUALS;
        Object value = TestData.getObject();
        Criteria criteria = Criteria.where().key(key).operator(operator)
                .value(value).build();
        List<Symbol> symbols = Parsing.groupExpressions(criteria.getSymbols());
        Expression exp = (Expression) symbols.get(0);
        Assert.assertEquals(1, symbols.size());
        Assert.assertEquals(exp.raw().key(), key);
        Assert.assertEquals(exp.raw().operator(), operator);
        Assert.assertEquals(exp.values().get(0).value(), value);
    }

    @Test
    public void testGroupAnd() {
        String key0 = TestData.getString();
        Operator operator0 = Operator.EQUALS;
        Object value0 = TestData.getObject();
        String key1 = TestData.getString();
        Operator operator1 = Operator.GREATER_THAN;
        Object value1 = TestData.getObject();
        Criteria criteria = Criteria.where().key(key0).operator(operator0)
                .value(value0).and().key(key1).operator(operator1).value(value1)
                .build();
        List<Symbol> symbols = Parsing.groupExpressions(criteria.getSymbols());
        Expression exp0 = (Expression) symbols.get(0);
        ConjunctionSymbol sym = (ConjunctionSymbol) symbols.get(1);
        Expression exp1 = (Expression) symbols.get(2);
        Assert.assertEquals(3, symbols.size());
        Assert.assertEquals(exp0.raw().key(), key0);
        Assert.assertEquals(exp0.raw().operator(), operator0);
        Assert.assertEquals(exp0.values().get(0).value(), value0);
        Assert.assertEquals(sym, ConjunctionSymbol.AND);
        Assert.assertEquals(exp1.raw().key(), key1);
        Assert.assertEquals(exp1.raw().operator(), operator1);
        Assert.assertEquals(exp1.values().get(0).value(), value1);
    }

    @Test
    public void testGroupOr() {
        String key0 = TestData.getString();
        Operator operator0 = Operator.EQUALS;
        Object value0 = TestData.getObject();
        String key1 = TestData.getString();
        Operator operator1 = Operator.GREATER_THAN;
        Object value1 = TestData.getObject();
        Criteria criteria = Criteria.where().key(key0).operator(operator0)
                .value(value0).or().key(key1).operator(operator1).value(value1)
                .build();
        List<Symbol> symbols = Parsing.groupExpressions(criteria.getSymbols());
        Expression exp0 = (Expression) symbols.get(0);
        ConjunctionSymbol sym = (ConjunctionSymbol) symbols.get(1);
        Expression exp1 = (Expression) symbols.get(2);
        Assert.assertEquals(3, symbols.size());
        Assert.assertEquals(exp0.raw().key(), key0);
        Assert.assertEquals(exp0.raw().operator(), operator0);
        Assert.assertEquals(exp0.values().get(0).value(), value0);
        Assert.assertEquals(sym, ConjunctionSymbol.OR);
        Assert.assertEquals(exp1.raw().key(), key1);
        Assert.assertEquals(exp1.raw().operator(), operator1);
        Assert.assertEquals(exp1.values().get(0).value(), value1);
    }

    @Test(expected = SyntaxException.class)
    public void testGroupSyntaxException() {
        List<Symbol> symbols = Lists.<Symbol> newArrayList(new KeySymbol("foo"),
                new KeySymbol("bar"));
        Parsing.groupExpressions(symbols);
    }

    @Test
    public void testGroupSub() {
        String key0 = TestData.getString();
        Operator operator0 = Operator.EQUALS;
        Object value0 = TestData.getObject();
        String key1 = TestData.getString();
        Operator operator1 = Operator.GREATER_THAN;
        Object value1 = TestData.getObject();
        String key2 = TestData.getString();
        Operator operator2 = Operator.LESS_THAN;
        Object value2 = TestData.getObject();
        Criteria criteria = Criteria.where().key(key0).operator(operator0)
                .value(value0).and()
                .group(Criteria.where().key(key1).operator(operator1)
                        .value(value1).or().key(key2).operator(operator2)
                        .value(value2).build())
                .build();
        List<Symbol> symbols = Parsing.groupExpressions(criteria.getSymbols());
        Expression exp0 = (Expression) symbols.get(0);
        ConjunctionSymbol sym1 = (ConjunctionSymbol) symbols.get(1);
        ParenthesisSymbol sym2 = (ParenthesisSymbol) symbols.get(2);
        Expression exp3 = (Expression) symbols.get(3);
        ConjunctionSymbol sym4 = (ConjunctionSymbol) symbols.get(4);
        Expression exp5 = (Expression) symbols.get(5);
        ParenthesisSymbol sym6 = (ParenthesisSymbol) symbols.get(6);
        Assert.assertEquals(7, symbols.size());
        Assert.assertEquals(exp0.raw().key(), key0);
        Assert.assertEquals(exp0.raw().operator(), operator0);
        Assert.assertEquals(exp0.values().get(0).value(), value0);
        Assert.assertEquals(ConjunctionSymbol.AND, sym1);
        Assert.assertEquals(ParenthesisSymbol.LEFT, sym2);
        Assert.assertEquals(exp3.raw().key(), key1);
        Assert.assertEquals(exp3.raw().operator(), operator1);
        Assert.assertEquals(exp3.values().get(0).value(), value1);
        Assert.assertEquals(ConjunctionSymbol.OR, sym4);
        Assert.assertEquals(exp5.raw().key(), key2);
        Assert.assertEquals(exp5.raw().operator(), operator2);
        Assert.assertEquals(exp5.values().get(0).value(), value2);
        Assert.assertEquals(ParenthesisSymbol.RIGHT, sym6);
    }

    @Test
    public void testGroupSingleBetween() {
        String key = TestData.getString();
        Operator operator = Operator.BETWEEN;
        Object value = TestData.getObject();
        Object value1 = TestData.getObject();
        Criteria criteria = Criteria.where().key(key).operator(operator)
                .value(value).value(value1).build();
        List<Symbol> symbols = Parsing.groupExpressions(criteria.getSymbols());
        Expression exp = (Expression) symbols.get(0);
        Assert.assertEquals(1, symbols.size());
        Assert.assertEquals(exp.raw().key(), key);
        Assert.assertEquals(exp.raw().operator(), operator);
        Assert.assertEquals(exp.values().get(0).value(), value);
        Assert.assertEquals(exp.values().get(1).value(), value1);
    }

    @Test
    public void testToPostfixNotationSimple() {
        Criteria criteria = Criteria.where().key("foo")
                .operator(Operator.EQUALS).value("bar").build();
        Queue<PostfixNotationSymbol> pfn = Parsing
                .toPostfixNotation(criteria.getSymbols());
        Assert.assertEquals(pfn.size(), 1);
        Assert.assertEquals(((Expression) Iterables.getOnlyElement(pfn)).key(),
                new KeySymbol("foo"));
        Assert.assertEquals(
                ((Expression) Iterables.getOnlyElement(pfn)).values().get(0),
                new ValueSymbol("bar"));
        Assert.assertEquals(
                ((Expression) Iterables.getOnlyElement(pfn)).operator(),
                new OperatorSymbol(Operator.EQUALS));
    }

    @Test
    public void testParseCclSimple() {
        Criteria criteria = Criteria.where().key("foo")
                .operator(Operator.EQUALS).value("bar").build();
        String ccl = "where foo = bar";
        Parser parser = Parsers.create(ccl);
        Assert.assertEquals(Parsing.toPostfixNotation(criteria.getSymbols()),
                parser.order());

    }

    @Test
    public void testToPostfixNotationSimpleBetween() {
        Criteria criteria = Criteria.where().key("foo")
                .operator(Operator.BETWEEN).value("bar").value("baz").build();
        Queue<PostfixNotationSymbol> pfn = Parsing
                .toPostfixNotation(criteria.getSymbols());
        Assert.assertEquals(pfn.size(), 1);
        Assert.assertEquals(((Expression) Iterables.getOnlyElement(pfn)).key(),
                new KeySymbol("foo"));
        Assert.assertEquals(
                ((Expression) Iterables.getOnlyElement(pfn)).values().get(0),
                new ValueSymbol("bar"));
        Assert.assertEquals(
                ((Expression) Iterables.getOnlyElement(pfn)).values().get(1),
                new ValueSymbol("baz"));
        Assert.assertEquals(
                ((Expression) Iterables.getOnlyElement(pfn)).operator(),
                new OperatorSymbol(Operator.BETWEEN));
    }

    @Test
    public void testParseCclBetween() {
        Criteria criteria = Criteria.where().key("foo")
                .operator(Operator.BETWEEN).value("bar").value("baz").build();
        String ccl = "where foo bw bar baz";
        String ccl2 = "where foo >< bar baz";
        Parser parser = Parsers.create(ccl);
        Assert.assertEquals(Parsing.toPostfixNotation(criteria.getSymbols()),
                parser.order());
        Parser parser2 = Parsers.create(ccl2);
        Assert.assertEquals(Parsing.toPostfixNotation(criteria.getSymbols()),
                parser2.order());
    }

    @Test
    public void testToPostfixNotationSimpleAnd() {
        Criteria criteria = Criteria.where().key("a").operator(Operator.EQUALS)
                .value(1).and().key("b").operator(Operator.EQUALS).value(2)
                .build();
        Queue<PostfixNotationSymbol> pfn = Parsing
                .toPostfixNotation(criteria.getSymbols());
        Assert.assertEquals(pfn.size(), 3);
        Assert.assertEquals(((Expression) Iterables.get(pfn, 0)),
                new Expression(new KeySymbol("a"),
                        new OperatorSymbol(Operator.EQUALS),
                        new ValueSymbol(1)));
        Assert.assertEquals(((Expression) Iterables.get(pfn, 1)),
                new Expression(new KeySymbol("b"),
                        new OperatorSymbol(Operator.EQUALS),
                        new ValueSymbol(2)));
        Assert.assertEquals(Iterables.get(pfn, 2), ConjunctionSymbol.AND);
    }

    @Test
    public void testParseCclSimpleAnd() {
        Criteria criteria = Criteria.where().key("a").operator(Operator.EQUALS)
                .value(1).and().key("b").operator(Operator.EQUALS).value(2)
                .build();
        String ccl = "a = 1 and b = 2";
        Parser parser = Parsers.create(ccl);
        Assert.assertEquals(Parsing.toPostfixNotation(criteria.getSymbols()),
                parser.order());
    }

    @Test
    public void testToPostfixNotationSimpleOr() {
        Criteria criteria = Criteria.where().key("a").operator(Operator.EQUALS)
                .value(1).or().key("b").operator(Operator.EQUALS).value(2)
                .build();
        Queue<PostfixNotationSymbol> pfn = Parsing
                .toPostfixNotation(criteria.getSymbols());
        Assert.assertEquals(pfn.size(), 3);
        Assert.assertEquals(((Expression) Iterables.get(pfn, 0)),
                new Expression(new KeySymbol("a"),
                        new OperatorSymbol(Operator.EQUALS),
                        new ValueSymbol(1)));
        Assert.assertEquals(((Expression) Iterables.get(pfn, 1)),
                new Expression(new KeySymbol("b"),
                        new OperatorSymbol(Operator.EQUALS),
                        new ValueSymbol(2)));
        Assert.assertEquals(Iterables.get(pfn, 2), ConjunctionSymbol.OR);
    }

    @Test
    public void testParseCclSimpleOr() {
        Criteria criteria = Criteria.where().key("a").operator(Operator.EQUALS)
                .value(1).or().key("b").operator(Operator.EQUALS).value(2)
                .build();
        String ccl = "a = 1 or b = 2";
        Parser parser = Parsers.create(ccl);
        Assert.assertEquals(Parsing.toPostfixNotation(criteria.getSymbols()),
                parser.order());
    }

    @Test
    public void testToPostfixNotationAndOr() {
        Criteria criteria = Criteria.where().key("a").operator(Operator.EQUALS)
                .value("1").and().key("b").operator(Operator.EQUALS).value(2)
                .or().key("c").operator(Operator.EQUALS).value(3).build();
        Queue<PostfixNotationSymbol> pfn = Parsing
                .toPostfixNotation(criteria.getSymbols());
        Assert.assertEquals(pfn.size(), 5);
        Assert.assertEquals(((Expression) Iterables.get(pfn, 0)),
                new Expression(new KeySymbol("a"),
                        new OperatorSymbol(Operator.EQUALS),
                        new ValueSymbol(1)));
        Assert.assertEquals(((Expression) Iterables.get(pfn, 1)),
                new Expression(new KeySymbol("b"),
                        new OperatorSymbol(Operator.EQUALS),
                        new ValueSymbol(2)));
        Assert.assertEquals(Iterables.get(pfn, 2), ConjunctionSymbol.AND);
        Assert.assertEquals(((Expression) Iterables.get(pfn, 3)),
                new Expression(new KeySymbol("c"),
                        new OperatorSymbol(Operator.EQUALS),
                        new ValueSymbol(3)));
        Assert.assertEquals(Iterables.get(pfn, 4), ConjunctionSymbol.OR);
    }

    @Test
    public void testParseCclAndOr() {
        Criteria criteria = Criteria.where().key("a").operator(Operator.EQUALS)
                .value("1").and().key("b").operator(Operator.EQUALS).value(2)
                .or().key("c").operator(Operator.EQUALS).value(3).build();
        String ccl = "a = 1 and b = 2 or c = 3";
        Parser parser = Parsers.create(ccl);
        Assert.assertEquals(Parsing.toPostfixNotation(criteria.getSymbols()),
                parser.order());
    }

    @Test
    public void testToPostfixNotationAndGroupOr() {
        Criteria criteria = Criteria.where().key("a").operator(Operator.EQUALS)
                .value(1).and()
                .group(Criteria.where().key("b").operator(Operator.EQUALS)
                        .value(2).or().key("c").operator(Operator.EQUALS)
                        .value(3).build())
                .build();
        Queue<PostfixNotationSymbol> pfn = Parsing
                .toPostfixNotation(criteria.getSymbols());
        Assert.assertEquals(((Expression) Iterables.get(pfn, 0)),
                new Expression(new KeySymbol("a"),
                        new OperatorSymbol(Operator.EQUALS),
                        new ValueSymbol(1)));
        Assert.assertEquals(((Expression) Iterables.get(pfn, 1)),
                new Expression(new KeySymbol("b"),
                        new OperatorSymbol(Operator.EQUALS),
                        new ValueSymbol(2)));
        Assert.assertEquals(((Expression) Iterables.get(pfn, 2)),
                new Expression(new KeySymbol("c"),
                        new OperatorSymbol(Operator.EQUALS),
                        new ValueSymbol(3)));
        Assert.assertEquals(Iterables.get(pfn, 3), ConjunctionSymbol.OR);
        Assert.assertEquals(Iterables.get(pfn, 4), ConjunctionSymbol.AND);

    }

    @Test
    public void testPostfixNotationAndGroupOr() {
        Criteria criteria = Criteria.where().key("a").operator(Operator.EQUALS)
                .value(1).and()
                .group(Criteria.where().key("b").operator(Operator.EQUALS)
                        .value(2).or().key("c").operator(Operator.EQUALS)
                        .value(3).build())
                .build();
        String ccl = "a = 1 and (b = 2 or c = 3)";
        Parser parser = Parsers.create(ccl);
        Assert.assertEquals(Parsing.toPostfixNotation(criteria.getSymbols()),
                parser.order());
    }

    @Test
    public void testToPostfixNotationGroupOrAndGroupOr() {
        Criteria criteria = Criteria.where()
                .group(Criteria.where().key("a").operator(Operator.EQUALS)
                        .value(1).or().key("b").operator(Operator.EQUALS)
                        .value(2).build())
                .and()
                .group(Criteria.where().key("c").operator(Operator.EQUALS)
                        .value(3).or().key("d").operator(Operator.EQUALS)
                        .value(4).build())
                .build();
        Queue<PostfixNotationSymbol> pfn = Parsing
                .toPostfixNotation(criteria.getSymbols());
        Assert.assertEquals(((Expression) Iterables.get(pfn, 0)),
                new Expression(new KeySymbol("a"),
                        new OperatorSymbol(Operator.EQUALS),
                        new ValueSymbol(1)));
        Assert.assertEquals(((Expression) Iterables.get(pfn, 1)),
                new Expression(new KeySymbol("b"),
                        new OperatorSymbol(Operator.EQUALS),
                        new ValueSymbol(2)));
        Assert.assertEquals(Iterables.get(pfn, 2), ConjunctionSymbol.OR);
        Assert.assertEquals(((Expression) Iterables.get(pfn, 3)),
                new Expression(new KeySymbol("c"),
                        new OperatorSymbol(Operator.EQUALS),
                        new ValueSymbol(3)));
        Assert.assertEquals(((Expression) Iterables.get(pfn, 4)),
                new Expression(new KeySymbol("d"),
                        new OperatorSymbol(Operator.EQUALS),
                        new ValueSymbol(4)));
        Assert.assertEquals(Iterables.get(pfn, 5), ConjunctionSymbol.OR);
        Assert.assertEquals(Iterables.get(pfn, 6), ConjunctionSymbol.AND);

    }

    @Test
    public void testParseCclGroupOrAndGroupOr() {
        Criteria criteria = Criteria.where()
                .group(Criteria.where().key("a").operator(Operator.EQUALS)
                        .value(1).or().key("b").operator(Operator.EQUALS)
                        .value(2).build())
                .and()
                .group(Criteria.where().key("c").operator(Operator.EQUALS)
                        .value(3).or().key("d").operator(Operator.EQUALS)
                        .value(4).build())
                .build();
        String ccl = "(a = 1 or b = 2) AND (c = 3 or d = 4)";
        Parser parser = Parsers.create(ccl);
        Assert.assertEquals(Parsing.toPostfixNotation(criteria.getSymbols()),
                parser.order());
    }

    @Test
    public void testParseCclGroupOrAndGroupOrConjuctions() {
        Criteria criteria = Criteria.where()
                .group(Criteria.where().key("a").operator(Operator.EQUALS)
                        .value(1).or().key("b").operator(Operator.EQUALS)
                        .value(2).build())
                .and()
                .group(Criteria.where().key("c").operator(Operator.EQUALS)
                        .value(3).or().key("d").operator(Operator.EQUALS)
                        .value(4).build())
                .build();
        String ccl = "(a = 1 || b = 2) && (c = 3 || d = 4)";
        Parser parser = Parsers.create(ccl);
        Assert.assertEquals(Parsing.toPostfixNotation(criteria.getSymbols()),
                parser.order());

    }

    @Test
    public void testParseCclGroupOrAndGroupOrConjuctionsWithSingleAmpersand() {
        Criteria criteria = Criteria.where()
                .group(Criteria.where().key("a").operator(Operator.EQUALS)
                        .value(1).or().key("b").operator(Operator.EQUALS)
                        .value(2).build())
                .and()
                .group(Criteria.where().key("c").operator(Operator.EQUALS)
                        .value(3).or().key("d").operator(Operator.EQUALS)
                        .value(4).build())
                .build();
        String ccl = "(a = 1 || b = 2) & (c = 3 || d = 4)";
        Parser parser = Parsers.create(ccl);
        Assert.assertEquals(Parsing.toPostfixNotation(criteria.getSymbols()),
                parser.order());
    }

    @Test
    public void testToPostfixNotationGroupOrOrGroupOr() {
        Criteria criteria = Criteria.where()
                .group(Criteria.where().key("a").operator(Operator.EQUALS)
                        .value(1).or().key("b").operator(Operator.EQUALS)
                        .value(2).build())
                .or()
                .group(Criteria.where().key("c").operator(Operator.EQUALS)
                        .value(3).or().key("d").operator(Operator.EQUALS)
                        .value(4).build())
                .build();
        Queue<PostfixNotationSymbol> pfn = Parsing
                .toPostfixNotation(criteria.getSymbols());
        Assert.assertEquals(((Expression) Iterables.get(pfn, 0)),
                new Expression(new KeySymbol("a"),
                        new OperatorSymbol(Operator.EQUALS),
                        new ValueSymbol(1)));
        Assert.assertEquals(((Expression) Iterables.get(pfn, 1)),
                new Expression(new KeySymbol("b"),
                        new OperatorSymbol(Operator.EQUALS),
                        new ValueSymbol(2)));
        Assert.assertEquals(Iterables.get(pfn, 2), ConjunctionSymbol.OR);
        Assert.assertEquals(((Expression) Iterables.get(pfn, 3)),
                new Expression(new KeySymbol("c"),
                        new OperatorSymbol(Operator.EQUALS),
                        new ValueSymbol(3)));
        Assert.assertEquals(((Expression) Iterables.get(pfn, 4)),
                new Expression(new KeySymbol("d"),
                        new OperatorSymbol(Operator.EQUALS),
                        new ValueSymbol(4)));
        Assert.assertEquals(Iterables.get(pfn, 5), ConjunctionSymbol.OR);
        Assert.assertEquals(Iterables.get(pfn, 6), ConjunctionSymbol.OR);

    }

    @Test
    public void testParseCclGroupOrOrGroupOr() {
        Criteria criteria = Criteria.where()
                .group(Criteria.where().key("a").operator(Operator.EQUALS)
                        .value(1).or().key("b").operator(Operator.EQUALS)
                        .value(2).build())
                .or()
                .group(Criteria.where().key("c").operator(Operator.EQUALS)
                        .value(3).or().key("d").operator(Operator.EQUALS)
                        .value(4).build())
                .build();
        String ccl = "(a = 1 or b = 2) or (c = 3 or d = 4)";
        Parser parser = Parsers.create(ccl);
        Assert.assertEquals(Parsing.toPostfixNotation(criteria.getSymbols()),
                parser.order());
    }

    @Test
    public void testParseCclGroupOrOrConjuction() {
        Criteria criteria = Criteria.where()
                .group(Criteria.where().key("a").operator(Operator.EQUALS)
                        .value(1).or().key("b").operator(Operator.EQUALS)
                        .value(2).build())
                .or()
                .group(Criteria.where().key("c").operator(Operator.EQUALS)
                        .value(3).or().key("d").operator(Operator.EQUALS)
                        .value(4).build())
                .build();
        String ccl = "(a = 1 || b = 2) || (c = 3 || d = 4)";
        Parser parser = Parsers.create(ccl);
        Assert.assertEquals(Parsing.toPostfixNotation(criteria.getSymbols()),
                parser.order());
    }

    @Test
    public void testParseCclTimestampComplexPhrase() {
        String ccl = "name = jeff at \"last christmas\"";
        Parser parser = Parsers.create(ccl);
        Queue<PostfixNotationSymbol> symbols = parser.order();
        Expression expr = (Expression) symbols.poll();
        Assert.assertNotEquals(0, expr.raw().timestamp()); // this means a
                                                           // timestamp was
                                                           // parsed
    }

    @Test
    public void testParseCclTimestampBasicPhrase() {
        String ccl = "name = jeff at \"now\"";
        Parser parser = Parsers.create(ccl);
        Queue<PostfixNotationSymbol> symbols = parser.order();
        Expression expr = (Expression) symbols.poll();
        Assert.assertNotEquals(0, expr.raw().timestamp()); // this means a
                                                           // timestamp was
                                                           // parsed
    }

    @Test
    public void testParseCclTimestampNumericPhrase() {
        String ccl = "name = jeff at \"" + Time.now() + "\"";
        Parser parser = Parsers.create(ccl);
        Queue<PostfixNotationSymbol> symbols = parser.order();
        Expression expr = (Expression) symbols.poll();
        Assert.assertNotEquals(0, expr.raw().timestamp()); // this means a
                                                           // timestamp was
                                                           // parsed
    }

    @Test
    public void testParseCclTimestampPhraseWithoutQuotes() {
        String ccl = "name = jeff at 3 seconds ago";
        Parser parser = Parsers.create(ccl);
        Queue<PostfixNotationSymbol> symbols = parser.order();
        Expression expr = (Expression) symbols.poll();
        Assert.assertNotEquals(0, expr.raw().timestamp()); // this means a
                                                           // timestamp was
                                                           // parsed
    }

    @Test
    public void testParseCclValueWithoutQuotes() {
        String ccl = "name = jeff nelson";
        Parser parser = Parsers.create(ccl);
        Queue<PostfixNotationSymbol> symbols = parser.order();
        Expression expr = (Expression) symbols.poll();
        Assert.assertEquals("jeff nelson", expr.values().get(0).value());
    }

    @Test
    public void testParseCclValueAndTimestampPhraseWithoutQuotes() {
        String ccl = "name = jeff nelson on last christmas day";
        Parser parser = Parsers.create(ccl);
        Queue<PostfixNotationSymbol> symbols = parser.order();
        Expression expr = (Expression) symbols.poll();
        Assert.assertEquals("jeff nelson", expr.values().get(0).value());
        Assert.assertNotEquals(0, expr.raw().timestamp()); // this means a
                                                           // timestamp was
                                                           // parsed
    }

    @Test
    public void testParseCclValueWithoutQuotesAnd() {
        String ccl = "name = jeff nelson and favorite_player != Lebron James";
        Parser parser = Parsers.create(ccl);
        Queue<PostfixNotationSymbol> symbols = parser.order();
        Assert.assertEquals(3, symbols.size());
        for (int i = 0; i < 2; ++i) {
            Expression expr = (Expression) symbols.poll();
            Assert.assertTrue(
                    expr.values().get(0).value().toString().contains(" "));
        }
    }

    @Test
    public void testParseCclValueAndTimestampPhraseWithoutQuotesAnd() {
        String ccl = "name = jeff nelson on last christmas day and favorite_player != Lebron James during last week";
        Parser parser = Parsers.create(ccl);
        Queue<PostfixNotationSymbol> symbols = parser.order();
        Assert.assertEquals(3, symbols.size());
        for (int i = 0; i < 2; ++i) {
            Expression expr = (Expression) symbols.poll();
            Assert.assertTrue(
                    expr.values().get(0).value().toString().contains(" "));
            Assert.assertNotEquals(0, expr.raw().timestamp()); // this means a
                                                               // timestamp was
                                                               // parsed
        }
    }

    @Test
    public void testParseCCLConjuctionsWithAnd() {
        String ccl = "name = chandresh pancholi on last christmas day && favovite_player != C. Ronaldo during last year";
        Parser parser = Parsers.create(ccl);
        Queue<PostfixNotationSymbol> symbols = parser.order();
        Assert.assertEquals(3, symbols.size());
        for (int i = 0; i < 2; i++) {
            Expression expr = (Expression) symbols.poll();
            Assert.assertTrue(
                    expr.values().get(0).value().toString().contains(" "));
            Assert.assertNotEquals(0, expr.raw().timestamp());
        }
    }

    @Test
    public void testParseCclLocalReferences() {
        Criteria criteria = Criteria.where().key("name")
                .operator(Operator.EQUALS).value("Lebron James").build();
        String ccl = "name = $name";
        Multimap<String, Object> data = LinkedHashMultimap.create();
        data.put("name", "Lebron James");
        data.put("age", 30);
        data.put("team", "Cleveland Cavaliers");
        Parser parser = Parsers.create(ccl, data);
        Assert.assertEquals(Parsing.toPostfixNotation(criteria.getSymbols()),
                parser.order());
    }

    @Test(expected = SyntaxException.class)
    public void testParseCclReferenceNotFound() {
        String ccl = "name = $name";
        Multimap<String, Object> data = LinkedHashMultimap.create();
        data.put("age", 30);
        data.put("team", "Cleveland Cavaliers");
        Parser parser = Parsers.create(ccl, data);
        Parsing.toPostfixNotation(parser.tokenize());
    }

    @Test(expected = SyntaxException.class)
    public void testParseCclInvalidReference() {
        String ccl = "name = $name";
        Multimap<String, Object> data = LinkedHashMultimap.create();
        data.put("name", "Lebron James");
        data.put("name", "King James");
        data.put("age", 30);
        data.put("team", "Cleveland Cavaliers");
        Parser parser = Parsers.create(ccl, data);
        Parsing.toPostfixNotation(parser.tokenize());
    }

    @Test
    public void testParseCclBetweenWithBothReferences() {
        Criteria criteria = Criteria.where().key("age")
                .operator(Operator.BETWEEN).value("30").value("35").build();
        String ccl = "where age bw $age $retireAge";
        Multimap<String, Object> data = LinkedHashMultimap.create();
        data.put("name", "Lebron James");
        data.put("age", 30);
        data.put("retireAge", 35);
        data.put("team", "Cleveland Cavaliers");
        Parser parser = Parsers.create(ccl, data);
        Assert.assertEquals(Parsing.toPostfixNotation(criteria.getSymbols()),
                parser.order());
    }

    @Test
    public void testParseCclBetweenWithFirstReference() {
        Criteria criteria = Criteria.where().key("age")
                .operator(Operator.BETWEEN).value("30").value("100").build();
        String ccl = "where age bw $age 100";
        Multimap<String, Object> data = LinkedHashMultimap.create();
        data.put("name", "Lebron James");
        data.put("age", 30);
        data.put("team", "Cleveland Cavaliers");
        Parser parser = Parsers.create(ccl, data);
        Assert.assertEquals(Parsing.toPostfixNotation(criteria.getSymbols()),
                parser.order());
    }

    @Test
    public void testParseCclBetweenWithSecondReference() {
        Criteria criteria = Criteria.where().key("age")
                .operator(Operator.BETWEEN).value("5").value("30").build();
        String ccl = "where age bw 5 $age";
        Multimap<String, Object> data = LinkedHashMultimap.create();
        data.put("name", "Lebron James");
        data.put("age", 30);
        data.put("team", "Cleveland Cavaliers");
        Parser parser = Parsers.create(ccl, data);
        Assert.assertEquals(Parsing.toPostfixNotation(criteria.getSymbols()),
                parser.order());
    }

    @Test
    public void testReproGH_113() {
        String ccl = "location = \"Atlanta (HQ)\"";
        Parser parser = Parsers.create(ccl);
        Queue<PostfixNotationSymbol> symbols = parser.order();
        Assert.assertEquals(1, symbols.size());
        Expression expr = (Expression) symbols.poll();
        Assert.assertEquals("Atlanta (HQ)", expr.raw().values().get(0));
    }

    public void testParseCclNoSpaces() {
        String ccl = "name=jeff";
        Parser parser = Parsers.create(ccl);
        if(parser instanceof JavaCCParser) {
            parser.order();
            Assert.assertTrue(true); // lack of exception = success
        }
        else {
            try {
                parser.order();
                Assert.fail(); // lack of exception = failure
            }
            catch (IllegalStateException e) {
                Assert.assertTrue(true);
            }
        }
    }
}
