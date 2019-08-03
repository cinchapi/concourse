/*
 * Copyright (c) 2013-2019 Cinchapi Inc.
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
package com.cinchapi.concourse.thrift;

import java.util.List;
import java.util.stream.Collectors;

import com.cinchapi.common.base.Array;
import com.cinchapi.concourse.util.Convert;

/**
 * Utility class for {@link Operator Operators}.
 *
 * @author Jeff Nelson
 */
public final class Operators {

    /**
     * Evaluate whether the {@code firstOperand} satisfies the {@code operator}
     * in relation to the {@code remainingOperands}.
     * 
     * @param firstOperand
     * @param operator
     * @param remainingOperands
     * @return a boolean that indicates the truthiness of the the relationship
     *         among the {@code firstOperand}, the {@code operator} and the
     *         {@code remainingOperands}
     */
    public static boolean evaluate(Object firstOperand,
            com.cinchapi.ccl.type.Operator operator,
            List<Object> remainingOperands) {
        return evaluate(firstOperand,
                Convert.stringToOperator(operator.symbol()), remainingOperands);
    }

    /**
     * Evaluate whether the {@code firstOperand} satisfies the {@code operator}
     * in relation to the {@code remainingOperands}.
     * 
     * @param firstOperand
     * @param operator
     * @param remainingOperands
     * @return a boolean that indicates the truthiness of the the relationship
     *         among the {@code firstOperand}, the {@code operator} and the
     *         {@code remainingOperands}
     */
    public static boolean evaluate(Object firstOperand, Operator operator,
            List<Object> remainingOperands) {
        TObject tvalue = Convert.javaToThrift(firstOperand);
        TObject[] tvalues = remainingOperands.stream()
                .map(Convert::javaToThrift).collect(Collectors.toList())
                .toArray(Array.containing());
        return tvalue.is(operator, tvalues);
    }

    private Operators() {/* no-init */}

}
