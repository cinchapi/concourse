/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse.lang.ast;

import javax.annotation.concurrent.Immutable;

import com.cinchapi.concourse.lang.ConjunctionSymbol;
import com.google.common.collect.Iterables;

/**
 * A {@link ConjunctionTree} is an {@link AST} that holds a
 * {@link ConjunctionSymbol} and is flanked on the left and right, by exactly
 * two other {@link AST} nodes. The {@link ConjunctionTree} is used to represent
 * an expression that connects two other expressions in a logical manner (e.g
 * AND/OR)
 * 
 * @author Jeff Nelson
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
