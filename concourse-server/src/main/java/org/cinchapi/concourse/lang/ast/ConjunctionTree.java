/*
 * The MIT License (MIT)
 * 
 * 2013-2015 Jeff Nelson, Cinchapi Software Collective
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
package org.cinchapi.concourse.lang.ast;

import javax.annotation.concurrent.Immutable;

import org.cinchapi.concourse.lang.ConjunctionSymbol;

import com.google.common.collect.Iterables;

/**
 * A {@link ConjunctionTree} is an {@link AST} that holds a
 * {@link ConjunctionSymbol} and is flanked on the left and right, by exactly
 * two other {@link AST} nodes. The {@link ConjunctionTree} is used to represent
 * an expression that connects two other expressions in a logical manner (e.g
 * AND/OR)
 * 
 * @author jnelson
 */
@Immutable
public class ConjunctionTree extends AST {

    /**
     * Construct a new instance.
     * 
     * @param symbol
     * @param children
     */
    protected ConjunctionTree(ConjunctionSymbol symbol, AST left, AST right) {
        super(symbol, left, right);
    }

    /**
     * Return the left child of this {@link ConjunctionTree}.
     * 
     * @return the left child
     */
    public AST getLeftChild() {
        return Iterables.get(children(), 0);
    }

    /**
     * Return the right child of this {@link ConjunctionTree}.
     * 
     * @return the right child
     */
    public AST getRightChild() {
        return Iterables.get(children(), 1);
    }

}
