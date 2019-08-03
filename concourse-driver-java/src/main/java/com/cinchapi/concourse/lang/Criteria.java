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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import com.cinchapi.ccl.Parser;
import com.cinchapi.ccl.Parsing;
import com.cinchapi.ccl.SyntaxException;
import com.cinchapi.ccl.grammar.Expression;
import com.cinchapi.ccl.grammar.ParenthesisSymbol;
import com.cinchapi.ccl.grammar.Symbol;
import com.cinchapi.ccl.grammar.TimestampSymbol;
import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.ParseException;
import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.util.Parsers;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * A {@link Criteria} is an object that is used to encapsulate the semantics of
 * a complex query. Any any given time, objects of this class can exist in one
 * of two modes: {@code building} or {@code built}. When a Criteria is
 * {@code built}, it is guaranteed to represent a fully and well formed query
 * that can be processed. On the other hand, when a Criteria is {@code building}
 * it is in an incomplete state.
 * <p>
 * This class is the public interface to Criteria construction. It is meant to
 * be used in a chained manner, where the caller initially calls
 * {@link Criteria#where()} and continues to construct the Criteria using the
 * options available from each subsequently returned state.
 * </p>
 * 
 * @author Jeff Nelson
 */
public class Criteria implements Symbol {

    /**
     * Return a {@link Criteria} object that expresses the same as the
     * {@code ccl} statement.
     * 
     * @param ccl the CCL statement to parse
     * @return an equivalanet {@link Criteria} object
     */
    public static Criteria parse(String ccl) {
        Parser parser = Parsers.create(ccl);
        Criteria criteria = new Criteria();
        try {
            criteria.symbols = Lists.newArrayList(parser.tokenize());
            return criteria;
        }
        catch (Exception e) {
            if(e instanceof SyntaxException
                    || e instanceof IllegalStateException
                    || e.getCause() != null && e
                            .getCause() instanceof com.cinchapi.ccl.v2.generated.ParseException) {
                throw new ParseException(
                        new com.cinchapi.concourse.thrift.ParseException(
                                e.getMessage()));
            }
            else {
                throw CheckedExceptions.throwAsRuntimeException(e);
            }
        }
    }

    /**
     * Start building a new {@link Criteria}.
     * 
     * @return the Criteria builder
     */
    public static StartState where() {
        return new StartState(new Criteria());
    }

    /**
     * A flag that indicates whether this {@link Criteria} has been built.
     */
    private boolean built = false;

    /**
     * The collection of {@link Symbol}s that make up this {@link Criteria}.
     */
    private List<Symbol> symbols;

    /**
     * Construct a new instance.
     */
    protected Criteria() {
        this.symbols = Lists.newArrayList();
    }

    /**
     * Return this {@link Criteria} with each expression (e.g. {key} {operator}
     * {values}) pinned to the specified {@code timestamp}.
     * 
     * <strong>NOTE:</strong> Any timestamps that are pinned to any expressions
     * within this Criteria will be replaced by the specified {@code timestamp}.
     * 
     * @param timestamp the {@link Timestamp} to which the returned
     *            {@link Criteria} is pinned
     * 
     * @return this {@link Criteria} pinned to {@code timestamp}
     */
    public Criteria at(Timestamp timestamp) {
        Parser parser = Parsers.create(getCclString());
        List<Symbol> symbols = Parsing.groupExpressions(parser.tokenize());
        TimestampSymbol ts = new TimestampSymbol(timestamp.getMicros());
        symbols.forEach((symbol) -> {
            if(symbol instanceof Expression) {
                Expression expression = (Expression) symbol;
                Reflection.set("timestamp", ts, expression); // (authorized)
            }
        });
        Criteria criteria = new Criteria();
        symbols = Parsing.ungroupExpressions(symbols);
        criteria.symbols = symbols;
        return criteria;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Criteria) {
            return Objects.equals(symbols, ((Criteria) obj).symbols);
        }
        else {
            return false;
        }
    }

    /**
     * Return a CCL string that is equivalent to this object.
     * 
     * @return an equivalent CCL string
     */
    public String getCclString() {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Symbol symbol : symbols) {
            if(!first) {
                sb.append(" ");
            }
            sb.append(symbol);
            first = false;
        }
        return sb.toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(symbols);
    }

    @Override
    public String toString() {
        return getCclString();
    }

    /**
     * Add a {@link Symbol} to this {@link Criteria}.
     * 
     * @param symbol
     */
    protected void add(Symbol symbol) {
        Preconditions.checkState(!built,
                "Cannot add a symbol to a built Criteria");
        symbols.add(symbol);
    }

    /**
     * Mark this {@link Criteria} as {@code built}.
     */
    protected void close() {
        built = !built ? true : built;
        List<Symbol> expanded = Lists.newArrayList();
        expand(symbols, expanded);
        this.symbols = expanded;
    }

    /**
     * Return the order list of symbols that make up this {@link Criteria}.
     * 
     * @return symbols
     */
    protected List<Symbol> getSymbols() {
        return Collections.unmodifiableList(symbols);
    }

    /**
     * Expand any sub/grouped Criteria.
     * 
     * @param symbols
     * @param expanded
     */
    private void expand(List<Symbol> symbols, List<Symbol> expanded) {
        for (Symbol symbol : symbols) {
            if(symbol instanceof Criteria) {
                expanded.add(ParenthesisSymbol.LEFT);
                expand(((Criteria) symbol).symbols, expanded);
                expanded.add(ParenthesisSymbol.RIGHT);
            }
            else {
                expanded.add(symbol);
            }
        }
    }

}
