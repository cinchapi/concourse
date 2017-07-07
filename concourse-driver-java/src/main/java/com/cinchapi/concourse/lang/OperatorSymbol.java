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
package com.cinchapi.concourse.lang;

import java.text.MessageFormat;

import javax.annotation.concurrent.Immutable;

import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.util.Convert;

/**
 * A {@link Symbol} that represents an {@link Operator} in a {@link Criteria}.
 * 
 * @author Jeff Nelson
 */
@Immutable
class OperatorSymbol extends AbstractSymbol implements PostfixNotationSymbol {

    /**
     * Return the {@link OperatorSymbol} for the specified {@code operator}.
     * 
     * @param operator
     * @return the symbol
     */
    public static OperatorSymbol create(Operator operator) {
        return new OperatorSymbol(operator);
    }

    /**
     * Return the {@link OperatorSymbol} that is parsed from {@code string}.
     * 
     * @param string
     * @return the symbol
     */
    public static OperatorSymbol parse(String string) {
        try {
            return new OperatorSymbol(Convert.stringToOperator(string));
        }
        catch (IllegalArgumentException e) {
            throw new RuntimeException(MessageFormat.format(
                    "Cannot parse {0} into an OperatorSymbol", string));
        }
    }

    /**
     * The associated operator.
     */
    private final Operator operator;

    /**
     * Construct a new instance.
     * 
     * @param operator
     */
    private OperatorSymbol(Operator operator) {
        this.operator = operator;
    }

    /**
     * Return the operator represented by this {@link Symbol}.
     * 
     * @return the operator
     */
    public Operator getOperator() {
        return operator;
    }

    @Override
    public String toString() {
        return Convert.operatorToString(operator);
    }

}
