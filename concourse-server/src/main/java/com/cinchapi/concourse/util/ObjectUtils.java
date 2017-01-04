/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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
package com.cinchapi.concourse.util;

import java.util.Collection;

import javax.annotation.Nullable;

import com.google.common.base.Strings;

/**
 * Utilities for general Objects.
 * 
 * @author jnelson
 */
public class ObjectUtils {

    /**
     * Return {@code true} if {@code value} is {@code null} or considered empty.
     * A value is considered empty if:
     * <ul>
     * <li>It is a collection with no members</li>
     * <li>It is an array with a length of 0</li>
     * <li>It is a string with no characters</li>
     * </ul>
     * 
     * @param value
     * @return {@code true} if the object is considered null or empty
     */
    public static boolean isNullOrEmpty(Object value) {
        return value == null
                || (value instanceof Collection && ((Collection<?>) value)
                        .isEmpty())
                || (value.getClass().isArray() && ((Object[]) value).length == 0)
                || (value instanceof String && Strings
                        .isNullOrEmpty((String) value));
    }

    /**
     * This method is similar to {@link Objects#firstNonNull(Object, Object)}
     * except it takes an arbitrary number of arguments and it won't throw a NPE
     * of all the objects are null.
     * 
     * @param objects
     * @return the first non-null object
     */
    @SafeVarargs
    @Nullable
    public static <T> T firstNonNullOrNull(T... objects) {
        for (T object : objects) {
            if(object != null) {
                return object;
            }
        }
        return null;
    }
}
