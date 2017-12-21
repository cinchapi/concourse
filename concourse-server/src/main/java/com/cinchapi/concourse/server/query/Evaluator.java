/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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
package com.cinchapi.concourse.server.query;

import java.util.Collections;
import java.util.Set;

import com.cinchapi.ccl.grammar.ConjunctionSymbol;
import com.cinchapi.ccl.grammar.Expression;
import com.cinchapi.ccl.grammar.TimestampSymbol;
import com.cinchapi.ccl.syntax.AbstractSyntaxTree;
import com.cinchapi.ccl.syntax.ConjunctionTree;
import com.cinchapi.ccl.syntax.ExpressionTree;
import com.cinchapi.ccl.syntax.Visitor;
import com.cinchapi.common.base.Verify;
import com.cinchapi.concourse.server.storage.Store;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.util.Conversions;
import com.cinchapi.concourse.util.TSets;
import com.cinchapi.concourse.util.Transformers;

/**
 * An {@link AbstractSyntaxTree} {@link Visitor} that evaluates the represented
 * query and returns a result set.
 * 
 * @author Jeff Nelson
 */
public class Evaluator implements Visitor<Set<Long>> {

    /**
     * The singleton instance.
     */
    private static final Evaluator INSTANCE = new Evaluator();

    /**
     * Return an instance of this {@link Evaluator}.
     * 
     * @return the {@link Evaluator}
     */
    public static Evaluator instance() {
        return INSTANCE;
    }

    private Evaluator() {/* singleton */}

    @Override
    public Set<Long> visit(ConjunctionTree tree, Object... data) {
        if(tree.root() == ConjunctionSymbol.AND) {
            Set<Long> a;
            AbstractSyntaxTree bTree;
            // Attempt to short circuit by evaluating the leaf node first to see
            // if its result set is empty
            if(!tree.left().isLeaf() && tree.right().isLeaf()) {
                a = tree.right().accept(this, data);
                bTree = tree.left();
            }
            else {
                a = tree.left().accept(this, data);
                bTree = tree.right();
            }
            if(a.isEmpty()) {
                // Since the AND conjunction takes the intersection, we know
                // that the result set is empty, regardless of what evaluation
                // is done to the second branch
                return Collections.emptySet();
            }
            else {
                Set<Long> b = bTree.accept(this, data);
                Set<Long> results = TSets.intersection(a, b);
                return results;
            }
        }
        else {
            Set<Long> left = tree.left().accept(this, data);
            Set<Long> right = tree.right().accept(this, data);
            Set<Long> results = TSets.union(left, right);
            return results;
        }
    }

    @Override
    public Set<Long> visit(ExpressionTree tree, Object... data) {
        Verify.that(data.length >= 1);
        Verify.that(data[0] instanceof Store);
        Store store = (Store) data[0];
        Expression expression = ((Expression) tree.root());
        String key = expression.raw().key();
        Operator operator = (Operator) expression.raw().operator();
        TObject[] values = Transformers.transformArray(
                expression.raw().values().toArray(), Conversions.javaToThrift(),
                TObject.class);
        Set<Long> results = expression.timestamp() == TimestampSymbol.PRESENT
                ? store.find(key, operator, values)
                : store.find(expression.raw().timestamp(), key, operator,
                        values);
        return results;
    }

}
