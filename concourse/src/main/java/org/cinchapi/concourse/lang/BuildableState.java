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
 * The base class for a state that can build a complete and well-formed
 * {@link Criteria}.
 * 
 * @author jnelson
 */
public abstract class BuildableState extends State {

    /**
     * Construct a new instance.
     * 
     * @param criteria
     */
    protected BuildableState(Criteria criteria) {
        super(criteria);
    }

    /**
     * Build and return the {@link Criteria}.
     * 
     * @return the built Criteria
     */
    public final Criteria build() {
        criteria.close();
        return criteria;
    }
    
    /**
     * Build a conjunctive clause onto the {@link Criteria} that is building.
     * 
     * @return the builder
     */
    public StartState and() {
        criteria.add(ConjunctionSymbol.AND);
        return new StartState(criteria);
    }

    /**
     * Build a disjunctive clause onto the {@link Criteria} that is building.
     * 
     * @return the builder
     */
    public StartState or() {
        criteria.add(ConjunctionSymbol.OR);
        return new StartState(criteria);
    }

}
