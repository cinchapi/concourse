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

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javax.annotation.concurrent.Immutable;

import com.cinchapi.concourse.lang.Symbol;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;

/**
 * An {@link AST} is a generic abstract syntax tree that can be used to
 * represent a statement in the language.
 * <p>
 * Each {@link AST} contains a single {@link Symbol} and any number of
 * {@link #children} nodes. If the tree contains no children, it is considered a
 * "leaf". This property can be checked by the {@link #isLeaf()} method.
 * </p>
 * 
 * @author Jeff Nelson
 */
@Immutable
public abstract class AST {

    /**
     * The list of this AST's children in order from left to right.
     */
    private final List<AST> children = Lists.newArrayList();

    /**
     * The symbol that is held in this {@link AST}.
     */
    private final Symbol symbol;

    /**
     * Construct a new instance.
     * 
     * @param children
     */
    protected AST(Symbol symbol, AST... children) {
        this.symbol = symbol;
        for (AST child : children) {
            this.children.add(child);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if(obj.getClass() == this.getClass()) {
            return Objects.equal(symbol, ((AST) obj).symbol)
                    && Objects.equal(children, ((AST) obj).children);
        }
        return false;
    }

    /**
     * Return the {@link Symbol} that is contained in this {@link AST} node.
     * 
     * @return the symbol
     */
    public Symbol getSymbol() {
        return symbol;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(symbol, children);
    }

    /**
     * Return {@code true} if this {@link AST} has no children and is therefore
     * considered a leaf node.
     * 
     * @return {@code true} if this AST is considered a leaf node
     */
    public boolean isLeaf() {
        return children.isEmpty();
    }

    /**
     * Return an list of this {@link AST}'s children. The list will
     * contain the children of this node from left to right.
     * 
     * @return an iterator for the children
     */
    protected Collection<AST> children() {
        return Collections.unmodifiableList(children);
    }

}
