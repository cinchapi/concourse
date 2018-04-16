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

/**
 * When re-constructing the state of a record/field/index from some base state,
 * A {@link Diff} describes the {@link Action} necessary to perform using the
 * data from a {@link Write}.
 * 
 * @author dubex
 */
public enum Diff implements org.apache.thrift.TEnum {
    ADDED(1), REMOVED(2);

    private final int value;

    private Diff(int value) {
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
    public static Diff findByValue(int value) {
        switch (value) {
        case 1:
            return ADDED;
        case 2:
            return REMOVED;
        default:
            return null;
        }
    }
}
