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

import com.cinchapi.concourse.lang.Expression;

/**
 * A {@link ExpressionTree} is an {@link AST} that holds an {@link Expression}
 * symbol and does not have any children.
 * 
 * @author Jeff Nelson
 */
@Immutable
public class ExpressionTree extends AST {

    /**
     * Create a new {@link ExpressionTree}.
     * 
     * @param expression
     * @return the new ExpressionTree
     */
    public static ExpressionTree create(Expression expression) {
        return new ExpressionTree(expression);
    }

    /**
     * Construct a new instance.
     * 
     * @param symbol
     */
    protected ExpressionTree(Expression symbol) {
        super(symbol);
    }

}
