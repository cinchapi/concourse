/*
 * Copyright (c) 2013-2024 Cinchapi Inc.
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

import com.cinchapi.ccl.Parsing;
import com.cinchapi.ccl.grammar.ExpressionSymbol;
import com.cinchapi.ccl.grammar.ParenthesisSymbol;
import com.cinchapi.ccl.grammar.Symbol;
import com.cinchapi.ccl.grammar.TimestampSymbol;
import com.cinchapi.ccl.syntax.AbstractSyntaxTree;
import com.cinchapi.concourse.Timestamp;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * A {@link Criteria} that has been {@link BuildableState#build()}.
 *
 * @author Jeff Nelson
 */
public class BuiltCriteria implements Criteria {

    /**
     * A flag that indicates whether this {@link Criteria} has been built.
     */
    private boolean built = false;

    /**
     * The collection of {@link Symbol}s that make up this {@link Criteria}.
     */
    List<Symbol> symbols;

    /**
     * Construct a new instance.
     */
    protected BuiltCriteria() {
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
        AbstractSyntaxTree ast = ConcourseCompiler.get().parse(ccl());
        List<Symbol> symbols = Parsing
                .groupExpressions(ConcourseCompiler.get().tokenize(ast));
        TimestampSymbol ts = new TimestampSymbol(timestamp.getMicros());
        symbols.forEach((symbol) -> {
            if(symbol instanceof ExpressionSymbol) {
                ExpressionSymbol expression = (ExpressionSymbol) symbol;
                expression.timestamp(ts);
            }
        });
        BuiltCriteria criteria = new BuiltCriteria();
        symbols = Parsing.ungroupExpressions(symbols);
        criteria.symbols = symbols;
        return criteria;
    }

    @Override
    public String ccl() {
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
    public boolean equals(Object obj) {
        if(obj instanceof Criteria) {
            return Objects.equals(symbols, ((Criteria) obj).symbols());
        }
        else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(symbols);
    }

    @Override
    public List<Symbol> symbols() {
        return Collections.unmodifiableList(symbols);
    }

    @Override
    public String toString() {
        return ccl();
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
                expand(((Criteria) symbol).symbols(), expanded);
                expanded.add(ParenthesisSymbol.RIGHT);
            }
            else {
                expanded.add(symbol);
            }
        }
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

}
