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
 * Enumerates the possible TObject types
 */
public enum Type implements org.apache.thrift.TEnum {
    BOOLEAN(1),
    DOUBLE(2),
    FLOAT(3),
    INTEGER(4),
    LONG(5),
    LINK(6),
    STRING(7),
    TAG(8),
    NULL(9),
    TIMESTAMP(10);

    private final int value;

    private Type(int value) {
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
    public static Type findByValue(int value) {
        switch (value) {
        case 1:
            return BOOLEAN;
        case 2:
            return DOUBLE;
        case 3:
            return FLOAT;
        case 4:
            return INTEGER;
        case 5:
            return LONG;
        case 6:
            return LINK;
        case 7:
            return STRING;
        case 8:
            return TAG;
        case 9:
            return NULL;
        case 10:
            return TIMESTAMP;
        default:
            return null;
        }
    }
}
