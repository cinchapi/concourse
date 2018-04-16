/*
 * Copyright (c) 2013-2018 Cinchapi Inc.
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

import com.cinchapi.concourse.util.Convert;

/**
 * Enumerates the list of operators that can be used in criteria
 * specifications.
 */
public enum Operator
        implements org.apache.thrift.TEnum, com.cinchapi.ccl.type.Operator {
    REGEX(1),
    NOT_REGEX(2),
    EQUALS(3),
    NOT_EQUALS(4),
    GREATER_THAN(5),
    GREATER_THAN_OR_EQUALS(6),
    LESS_THAN(7),
    LESS_THAN_OR_EQUALS(8),
    BETWEEN(9),
    LINKS_TO(10),
    LIKE(11),
    NOT_LIKE(12);

    private final int value;

    private Operator(int value) {
        this.value = value;
    }

    /**
     * Get the integer value of this enum value, as defined in the Thrift IDL.
     */
    public int getValue() {
        return value;
    }

    /**
     * Find a the enum type by its integer value, as defined in the Thrift IDL.
     * 
     * @return null if the value is not found.
     */
    public static Operator findByValue(int value) {
        switch (value) {
        case 1:
            return REGEX;
        case 2:
            return NOT_REGEX;
        case 3:
            return EQUALS;
        case 4:
            return NOT_EQUALS;
        case 5:
            return GREATER_THAN;
        case 6:
            return GREATER_THAN_OR_EQUALS;
        case 7:
            return LESS_THAN;
        case 8:
            return LESS_THAN_OR_EQUALS;
        case 9:
            return BETWEEN;
        case 10:
            return LINKS_TO;
        case 11:
            return LIKE;
        case 12:
            return NOT_LIKE;
        default:
            return null;
        }
    }

    @Override
    public int operands() {
        return this == BETWEEN ? 2 : 1;
    }

    @Override
    public String symbol() {
        return Convert.operatorToString(this);
    }
}
