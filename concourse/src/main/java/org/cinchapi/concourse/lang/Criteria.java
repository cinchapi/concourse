/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2014 Jeff Nelson, Cinchapi Software Collective
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
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.cinchapi.concourse.lang;

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
 * {@link Criteria#builder()} and continues to construct the Criteria using the
 * options available from each subsequently returned state.
 * </p>
 * 
 * @author jnelson
 */
public class Criteria implements Symbol {

    /**
     * Start building a new {@link Criteria}.
     * 
     * @return the Criteria builder
     */
    public static StartState builder() {
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
     * Return the order list of symbols that make up this {@link Criteria}.
     * 
     * @return symbols
     */
    protected List<Symbol> getSymbols() {
        return Collections.unmodifiableList(symbols);
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

    @Override
    public String toString() {
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

}
