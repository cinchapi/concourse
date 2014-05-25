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


/**
 * This is the base class and marker for any valid state in the {@link Criteria}
 * builder. Each {@link State} is passed the current {@link Criteria} and holds
 * a reference. For any method called from the state, a {@link Symbol} is added
 * to the {@code Criteria} or the {@code Criteria} is returned.
 * <p>
 * For the purposes of a builder, a {@link State} typically describes what was
 * most recently consumed.
 * </p>
 * 
 * @author jnelson
 */
public abstract class State {

    /**
     * A reference to the {@link Criteria} that is being built.
     */
    protected final Criteria criteria;

    /**
     * Construct a new instance.
     * 
     * @param criteria
     */
    protected State(Criteria criteria) {
        this.criteria = criteria;
    }

}
