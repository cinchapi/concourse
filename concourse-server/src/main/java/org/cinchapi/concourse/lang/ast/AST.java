/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2015 Jeff Nelson, Cinchapi Software Collective
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

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javax.annotation.concurrent.Immutable;

import org.cinchapi.concourse.lang.Symbol;

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
 * @author jnelson
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
