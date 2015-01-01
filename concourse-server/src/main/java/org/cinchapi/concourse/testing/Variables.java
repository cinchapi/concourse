/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2015 Jeff Nelson, Cinchapi Software Collective
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.cinchapi.concourse.testing;

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
     * @param key
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
