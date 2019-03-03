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

/**
 * A representation for an enum that declares the type of a TSymbol.
 */
public enum TSymbolType implements org.apache.thrift.TEnum {
    CONJUNCTION(1), KEY(2), VALUE(3), PARENTHESIS(4), OPERATOR(5), TIMESTAMP(6);

    private final int value;

    private TSymbolType(int value) {
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
    public static TSymbolType findByValue(int value) {
        switch (value) {
        case 1:
            return CONJUNCTION;
        case 2:
            return KEY;
        case 3:
            return VALUE;
        case 4:
            return PARENTHESIS;
        case 5:
            return OPERATOR;
        case 6:
            return TIMESTAMP;
        default:
            return null;
        }
    }
}
