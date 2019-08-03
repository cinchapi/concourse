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
 * The possible types for a {@link ComplexTObject}.
 */
public enum ComplexTObjectType implements org.apache.thrift.TEnum {
    SCALAR(1), MAP(2), LIST(3), SET(4), TOBJECT(5), TCRITERIA(6), BINARY(7);

    private final int value;

    private ComplexTObjectType(int value) {
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
    public static ComplexTObjectType findByValue(int value) {
        switch (value) {
        case 1:
            return SCALAR;
        case 2:
            return MAP;
        case 3:
            return LIST;
        case 4:
            return SET;
        case 5:
            return TOBJECT;
        case 6:
            return TCRITERIA;
        case 7:
            return BINARY;
        default:
            return null;
        }
    }
}
