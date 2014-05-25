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
 * The {@link StateState} marks the logical beginning of a new {@link Criteria}.
 * 
 * @author jnelson
 */
public class StartState extends State {

    /**
     * Construct a new instance.
     * 
     * @param criteria
     */
    public StartState(Criteria criteria) {
        super(criteria);
    }

    /**
     * Add a sub {@code criteria} to the Criteria that is building. A sub
     * criteria is one that is wrapped in parenthesis.
     * 
     * @param criteria
     * @return the builder
     */
    public BuildableStartState group(Criteria criteria) {
        this.criteria.add(criteria);
        return new BuildableStartState(this.criteria);
    }

    /**
     * Add a {@code key} to the Criteria that is building.
     * 
     * @param key
     * @return the builder
     */
    public KeyState key(String key) {
        criteria.add(KeySymbol.create(key));
        return new KeyState(criteria);
    }

}
