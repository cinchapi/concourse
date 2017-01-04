/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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
