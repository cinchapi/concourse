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
package com.cinchapi.concourse.test;

import java.util.Map;

import javax.annotation.Nullable;

import com.google.common.collect.Maps;

/**
 * A static collection of variables. This class should be used to register test
 * variables so that they can be dumped in the event of a test failure.
 * 
 * @author jnelson
 */
public final class Variables {

    /**
     * Remove all the registered variables.
     */
    public static void clear() {
        vars.clear();
    }

    /**
     * Dump the variables to a formatted string.
     * 
     * @return a string with a dump of the variables.
     */
    public static String dump() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Object> entry : vars.entrySet()) {
            sb.append(entry.getKey() + " = " + entry.getValue() + " ("
                    + entry.getValue().getClass().getName() + ")");
            sb.append("\n");
        }
        return sb.toString();
    }

    /**
     * Return the value of {@code variable} if it is registered, {@code null}
     * otherwise. This method will throw a ClassCastException if it is not
     * possible to cast the value to T.
     * 
     * @param variable
     * @return the value of {@code variable}
     */
    @SuppressWarnings("unchecked")
    @Nullable
    public static <T> T get(String variable) {
        if(vars.containsKey(variable)) {
            return (T) vars.get(variable);
        }
        return null;
    }

    /**
     * Register {@code variable} as {@code value}.
     * 
     * @param variable
     * @param value
     * @return {@code value}
     */
    public static <T> T register(String variable, T value) {
        vars.put(variable, value);
        return value;
    }

    /**
     * The variables that are currently stored.
     */
    private final static Map<String, Object> vars = Maps.newLinkedHashMap();

}
