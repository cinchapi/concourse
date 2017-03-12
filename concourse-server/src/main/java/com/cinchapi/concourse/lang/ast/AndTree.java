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

import com.cinchapi.concourse.lang.ConjunctionSymbol;

/**
 * A {@link ConjunctionTree} that holds the AND {@link ConjunctionSymbol}.
 * 
 * @author Jeff Nelson
 */
public class AndTree extends ConjunctionTree {

    /**
     * Create a new {@link AndTree}.
     * 
     * @param left
     * @param right
     * @return the AndTree
     */
    public static AndTree create(AST left, AST right) {
        return new AndTree(left, right);
    }

    /**
     * Construct a new instance.
     * 
     * @param left
     * @param right
     */
    protected AndTree(AST left, AST right) {
        super(ConjunctionSymbol.AND, left, right);
    }

}
