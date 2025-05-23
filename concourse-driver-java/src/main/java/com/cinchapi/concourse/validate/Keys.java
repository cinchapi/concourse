/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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
package com.cinchapi.concourse.validate;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.cinchapi.ccl.grammar.FunctionKeySymbol;
import com.cinchapi.ccl.grammar.Symbol;
import com.cinchapi.ccl.syntax.AbstractSyntaxTree;
import com.cinchapi.ccl.syntax.FunctionTree;
import com.cinchapi.ccl.type.Function;
import com.cinchapi.ccl.type.function.ImplicitKeyRecordFunction;
import com.cinchapi.concourse.Constants;
import com.cinchapi.concourse.lang.ConcourseCompiler;
import com.google.common.base.Preconditions;

/**
 * Utility functions for data keys.
 *
 * @author Jeff Nelson
 */
public final class Keys {

    /**
     * Return {@code true} if {@code key} is a function key.
     * 
     * @param key
     * @return {@code true} if the provided {@code key} is a function key
     */
    public static boolean isFunctionKey(String key) {
        return parse(key).type() == KeyType.FUNCTION_KEY;
    }

    /**
     * Return {@code true} if {@code key} is a navigation key.
     * 
     * @param key
     * @return {@code true} if the provided {@code key} is a valid navigation
     *         key
     */
    public static boolean isNavigationKey(String key) {
        return parse(key).type() == KeyType.NAVIGATION_KEY;
    }

    /**
     * Return {@code true} of the {@code key} is a valid data key for writing.
     * 
     * @param key
     * @return {@code true} if the provided {@code key} is valid for writing
     */
    public static boolean isWritable(String key) {
        return parse(key).type() == KeyType.WRITABLE_KEY;
    }

    /**
     * Parse {@code key} and return a {@link Key} object that contains the
     * relevant information.
     * 
     * @param key
     * @return the parsed {@link Key}
     */
    public static Key parse(String key) {
        Preconditions.checkArgument(key != null, "Cannot parse a null key");
        return CACHE.computeIfAbsent(key, $ -> {
            if(key.length() > 0
                    && KEY_VALIDATION_REGEX.matcher(key).matches()) {
                return new Key(key, KeyType.WRITABLE_KEY, key);
            }
            else if(key.equals(Constants.JSON_RESERVED_IDENTIFIER_NAME)) {
                return new Key(key, KeyType.IDENTIFIER_KEY, key);
            }
            else {
                String[] toks = key.split("\\.");
                if(toks.length > 1) {
                    return new Key(key, KeyType.NAVIGATION_KEY, toks);
                }
                ImplicitKeyRecordFunction func = tryParseFunction(key);
                if(func != null) {
                    return new Key(key, KeyType.FUNCTION_KEY, func);
                }
                return new Key(key, KeyType.INVALID_KEY, key);
            }
        });
    }

    /**
     * If possible, parse the {@link Function} that is expressed by the
     * {@code key}.
     * 
     * @param key
     * @return the parsed {@link Function}, if possible, otherwise {@code null}
     */
    @Nullable
    public static ImplicitKeyRecordFunction tryParseFunction(String key) {
        if(key.indexOf("|") > 0) {
            AbstractSyntaxTree ast = ConcourseCompiler.get().parse(key);
            if(ast instanceof FunctionTree) {
                Symbol symbol = ast.root();
                if(symbol instanceof FunctionKeySymbol) {
                    Function function = ((FunctionKeySymbol) symbol).function();
                    if(function instanceof ImplicitKeyRecordFunction) {
                        return (ImplicitKeyRecordFunction) function;
                    }
                }
            }

        }
        return null;
    }

    /**
     * A global set of all inquired about keys and the information parsed about
     * them.
     */
    private static final Map<String, Key> CACHE = new ConcurrentHashMap<>(100);

    /**
     * A pre-compiled regex pattern that is used to validate that each key is
     * non-empty, alphanumeric with no special characters other than underscore
     * (_).
     */
    private static final Pattern KEY_VALIDATION_REGEX = Pattern
            .compile("^[a-zA-Z0-9_]+$");

    private Keys() {/* no-init */}

    /**
     * A {@link Keys#parse(String) parsed} key that includes information about
     * it's {@link KeyType} and some other data that may be useful in further
     * processing.
     *
     * @author Jeff Nelson
     */
    @Immutable
    public static final class Key {

        /**
         * The {@link KeyType} of key.
         */
        private final KeyType type;

        /**
         * Data that may be useful in further processing.
         */
        private final Object data;

        /**
         * The original input
         */
        private final String value;

        /**
         * Construct a new instance.
         * 
         * @param type
         * @param data
         */
        private Key(String value, KeyType type, Object data) {
            this.value = value;
            this.type = type;
            this.data = data;
        }

        /**
         * Return the data associated with this {@link Key}.
         * <p>
         * For example, if this is a {@link KeyType#NAVIGATION_KEY}, this will
         * return a {@code String[]} containing all of the stops in the path.
         * Or, if this is a {@link KeyType#FUNCTION_KEY}, this will return the
         * result of {@link Keys#tryParseFunction(String)}.
         * </p>
         * 
         * @return the associated data
         */
        @SuppressWarnings("unchecked")
        public <T> T data() {
            return (T) data;
        }

        @Override
        public String toString() {
            return value;
        }

        /**
         * Return the {@link KeyType}.
         * 
         * @return the type
         */
        public KeyType type() {
            return type;
        }

        /**
         * Return the value of this {@link Key}.
         * 
         * @return the value
         */
        public String value() {
            return value;
        }

    }

    /**
     * The various kinds of keys that can be {@link Keys#parse(String) parsed}.
     *
     * @author Jeff Nelson
     */
    public static enum KeyType {
        /**
         * A key that can be written and therefore read directly from a Store
         */
        WRITABLE_KEY,

        /**
         * A key that has multiple "stops" separated by a period (e.g.,
         * friends.partner.name)
         */
        NAVIGATION_KEY,

        /**
         * A key that instructs that a function should be evaluated
         */
        FUNCTION_KEY,

        /**
         * A key that refers to a record identifier
         */
        IDENTIFIER_KEY,

        /**
         * A key that cannot be parsed
         */
        INVALID_KEY

    }
}
