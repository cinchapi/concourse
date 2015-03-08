/*
 * Copyright (c) 2013-2015 Cinchapi, Inc.
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
package org.cinchapi.concourse.lang;

import java.text.MessageFormat;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Queue;

import org.cinchapi.concourse.lang.ast.AST;
import org.cinchapi.concourse.lang.ast.AndTree;
import org.cinchapi.concourse.lang.ast.ExpressionTree;
import org.cinchapi.concourse.lang.ast.OrTree;
import org.cinchapi.concourse.thrift.Operator;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * The {@link Parser} is a tool that operates on various aspects of the
 * language.
 * 
 * @author jnelson
 */
public final class Parser {

    /**
     * Convert a valid and well-formed list of {@link Symbol} objects into a
     * an {@link AST}.
     * <p>
     * NOTE: This method will group non-conjunctive symbols into
     * {@link Expression} objects.
     * </p>
     * 
     * @param symbols
     * @return the symbols in an AST
     */
    public static AST toAbstractSyntaxTree(List<Symbol> symbols) {
        Deque<Symbol> operatorStack = new ArrayDeque<Symbol>();
        Deque<AST> operandStack = new ArrayDeque<AST>();
        symbols = groupExpressions(symbols);
        main: for (Symbol symbol : symbols) {
            if(symbol == ParenthesisSymbol.LEFT) {
                operatorStack.push(symbol);
            }
            else if(symbol == ParenthesisSymbol.RIGHT) {
                while (!operatorStack.isEmpty()) {
                    Symbol popped = operatorStack.pop();
                    if(popped == ParenthesisSymbol.LEFT) {
                        continue main;
                    }
                    else {
                        addASTNode(operandStack, popped);
                    }
                }
                throw new SyntaxException(MessageFormat.format(
                        "Syntax error in {0}: Mismatched parenthesis", symbols));
            }
            else if(symbol instanceof Expression) {
                operandStack.add(ExpressionTree.create((Expression) symbol));
            }
            else {
                operatorStack.push(symbol);
            }
        }
        while (!operatorStack.isEmpty()) {
            addASTNode(operandStack, operatorStack.pop());
        }
        return operandStack.pop();
    }

    /**
     * Convert a valid and well-formed list of {@link Symbol} objects into a
     * Queue in postfix notation.
     * <p>
     * NOTE: This method will group non-conjunctive symbols into
     * {@link Expression} objects.
     * </p>
     * 
     * @param symbols
     * @return the symbols in postfix notation
     */
    public static Queue<PostfixNotationSymbol> toPostfixNotation(
            List<Symbol> symbols) {
        Deque<Symbol> stack = new ArrayDeque<Symbol>();
        Queue<PostfixNotationSymbol> queue = new LinkedList<PostfixNotationSymbol>();
        symbols = groupExpressions(symbols);
        for (Symbol symbol : symbols) {
            if(symbol instanceof ConjunctionSymbol) {
                while (!stack.isEmpty()) {
                    Symbol top = stack.peek();
                    if(symbol == ConjunctionSymbol.OR
                            && (top == ConjunctionSymbol.OR || top == ConjunctionSymbol.AND)) {
                        queue.add((PostfixNotationSymbol) stack.pop());
                    }
                    else {
                        break;
                    }
                }
                stack.push(symbol);
            }
            else if(symbol == ParenthesisSymbol.LEFT) {
                stack.push(symbol);
            }
            else if(symbol == ParenthesisSymbol.RIGHT) {
                boolean foundLeftParen = false;
                while (!stack.isEmpty()) {
                    Symbol top = stack.peek();
                    if(top == ParenthesisSymbol.LEFT) {
                        foundLeftParen = true;
                        break;
                    }
                    else {
                        queue.add((PostfixNotationSymbol) stack.pop());
                    }
                }
                if(!foundLeftParen) {
                    throw new SyntaxException(MessageFormat.format(
                            "Syntax error in {0}: Mismatched parenthesis",
                            symbols));
                }
                else {
                    stack.pop();
                }
            }
            else {
                queue.add((PostfixNotationSymbol) symbol);
            }
        }
        while (!stack.isEmpty()) {
            Symbol top = stack.peek();
            if(top instanceof ParenthesisSymbol) {
                throw new SyntaxException(MessageFormat.format(
                        "Syntax error in {0}: Mismatched parenthesis", symbols));
            }
            else {
                queue.add((PostfixNotationSymbol) stack.pop());
            }
        }
        return queue;
    }

    /**
     * Go through a list of symbols and group the expressions together in a
     * {@link Expression} object.
     * 
     * @param symbols
     * @return the expression
     */
    protected static List<Symbol> groupExpressions(List<Symbol> symbols) { // visible
                                                                           // for
                                                                           // testing
        try {
            List<Symbol> grouped = Lists.newArrayList();
            ListIterator<Symbol> it = symbols.listIterator();
            while (it.hasNext()) {
                Symbol symbol = it.next();
                if(symbol instanceof KeySymbol) {
                    // NOTE: We are assuming that the list of symbols is well
                    // formed, and, as such, the next elements will be an
                    // operator and one or more symbols. If this is not the
                    // case, this method will throw a ClassCastException
                    OperatorSymbol operator = (OperatorSymbol) it.next();
                    ValueSymbol value = (ValueSymbol) it.next();
                    Expression expression;
                    if(operator.getOperator() == Operator.BETWEEN) {
                        ValueSymbol value2 = (ValueSymbol) it.next();
                        expression = Expression.create((KeySymbol) symbol,
                                operator, value, value2);
                    }
                    else {
                        expression = Expression.create((KeySymbol) symbol,
                                operator, value);
                    }
                    grouped.add(expression);
                }
                else if(symbol instanceof TimestampSymbol) { // Add the
                                                             // timestamp to the
                                                             // previously
                                                             // generated
                                                             // Expression
                    ((Expression) Iterables.getLast(grouped))
                            .setTimestamp((TimestampSymbol) symbol);
                }
                else {
                    grouped.add(symbol);
                }
            }
            return grouped;
        }
        catch (ClassCastException e) {
            throw new SyntaxException(e.getMessage());
        }
    }

    /**
     * An the appropriate {@link AST} node to the {@code stack} based on
     * {@code operator}.
     * 
     * @param stack
     * @param operator
     */
    private static void addASTNode(Deque<AST> stack, Symbol operator) {
        AST right = stack.pop();
        AST left = stack.pop();
        if(operator == ConjunctionSymbol.AND) {
            stack.push(AndTree.create(left, right));
        }
        else {
            stack.push(OrTree.create(left, right));
        }
    }

    private Parser() {/* noop */}

}
