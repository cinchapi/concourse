/*
 * Copyright (c) 2013-2023 Cinchapi Inc.
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

import com.cinchapi.ccl.SyntaxException;
import com.cinchapi.ccl.grammar.Symbol;
import com.cinchapi.ccl.syntax.AbstractSyntaxTree;
import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.concourse.ParseException;
import com.cinchapi.concourse.Timestamp;
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
public interface Criteria extends Symbol {

    /**
     * Return a {@link Criteria} object that expresses the same as the
     * {@code ccl} statement.
     * 
     * @param ccl the CCL statement to parse
     * @return an equivalent {@link Criteria} object
     */
    public static Criteria parse(String ccl) {
        try {
            AbstractSyntaxTree ast = ConcourseCompiler.get().parse(ccl);
            BuiltCriteria criteria = new BuiltCriteria();
            criteria.symbols = Lists
                    .newArrayList(ConcourseCompiler.get().tokenize(ast));
            return criteria;
        }
        catch (Exception e) {
            if(e instanceof SyntaxException
                    || e instanceof IllegalStateException
                    || e.getCause() != null && e
                            .getCause() instanceof com.cinchapi.ccl.generated.ParseException) {
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
        return new StartState(new BuiltCriteria());
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
    public Criteria at(Timestamp timestamp);

    /**
     * Return a CCL string that is equivalent to this object.
     * 
     * @return an equivalent CCL string
     */
    public String ccl();

    /**
     * Return a CCL string that is equivalent to this object.
     * 
     * @return an equivalent CCL string
     * @deprecated in favor of {@link #ccl()}
     */
    @Deprecated
    public default String getCclString() {
        return ccl();
    }

    /**
     * Return the order list of symbols that make up this {@link Criteria}.
     * 
     * @return symbols
     */
    public List<Symbol> symbols();

}
