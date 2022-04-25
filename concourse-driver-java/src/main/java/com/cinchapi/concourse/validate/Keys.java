/*
 * Copyright (c) 2013-2022 Cinchapi Inc.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import com.cinchapi.ccl.grammar.FunctionKeySymbol;
import com.cinchapi.ccl.grammar.Symbol;
import com.cinchapi.ccl.syntax.AbstractSyntaxTree;
import com.cinchapi.ccl.syntax.FunctionTree;
import com.cinchapi.ccl.type.Function;
import com.cinchapi.ccl.type.function.ImplicitKeyRecordFunction;
import com.cinchapi.concourse.Constants;
import com.cinchapi.concourse.lang.ConcourseCompiler;
import com.google.common.collect.ImmutableMap;

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
        return tryParseFunction(key) != null;
    }

    /**
     * Return {@code true} if {@code key} is a navigation key.
     * 
     * @param key
     * @return {@code true} if the provided {@code key} is a valid navigation
     *         key
     */
    public static boolean isNavigationKey(String key) {
        return key.indexOf('.') > 0;
    }

    /**
     * Return {@code true} of the {@code key} is a valid data key for writing.
     * 
     * @param key
     * @return {@code true} if the provided {@code key} is valid for writing
     */
    public static boolean isWritable(String key) {
        if(WRITABLE_KEY_CACHE.contains(key)) {
            return true;
        }
        else {
            boolean writable = key.length() > 0
                    && KEY_VALIDATION_REGEX.matcher(key).matches();
            if(writable) {
                WRITABLE_KEY_CACHE.add(key);
            }
            return writable;
        }

    }

    /**
     * Parse {@code key} and return a {@link Key} object that contains the
     * relevant information.
     * 
     * @param key
     * @return the parsed {@link Key}
     */
    public static Key parse(String key) {
        String[] toks;
        ImplicitKeyRecordFunction func;
        if(isWritable(key)) {
            return new Key(key, Type.WRITABLE_KEY, key);
        }
        else if(key.equals(Constants.JSON_RESERVED_IDENTIFIER_NAME)) {
            return new Key(key, Type.IDENTIFIER_KEY, key);
        }
        else {
            toks = key.split("\\.");
            if(toks.length > 1) {
                return new Key(key, Type.NAVIGATION_KEY, toks);
            }
            func = tryParseFunction(key);
            if(func != null) {
                return new Key(key, Type.FUNCTION_KEY, func);
            }
            return new Key(key, Type.INVALID_KEY, key);
        }
    }

    /**
     * Parse each of the {@code keys} and return a corresponding {@link Map}
     * from each parsed key's {@link Keys.Key#type()} to a {@link List} of
     * {@link Key} objects, each with relevant information, for all of the
     * {@code keys} that are of that {@link Type}.
     * 
     * @param key
     * @return the parsed {@link Key}
     */
    public static Map<Type, List<Key>> parse(Collection<String> keys) {
        // @formatter:off
        Map<Type, List<Key>> _keys = ImmutableMap.of(
                Type.FUNCTION_KEY, new ArrayList<>(0), 
                Type.IDENTIFIER_KEY, new ArrayList<>(0),
                Type.INVALID_KEY, new ArrayList<>(0), 
                Type.NAVIGATION_KEY, new ArrayList<>(0), 
                Type.WRITABLE_KEY, new ArrayList<>(keys.size())
        );
        // @formatter:on
        for (String key : keys) {
            Key _key = parse(key);
            _keys.get(_key.type()).add(_key);
        }
        return _keys;
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
     * A pre-compiled regex pattern that is used to validate that each key is
     * non-empty, alphanumeric with no special characters other than underscore
     * (_).
     */
    private static final Pattern KEY_VALIDATION_REGEX = Pattern
            .compile("^[a-zA-Z0-9_]+$");

    /**
     * A global set of all {@link String Strings} known to be
     * {@link #isWritable(String) writable} keys.
     */
    // We aren't concerned about thread safety here because items are never
    // removed from the cache and spurious lookup failure would merely trigger
    // manual validation.
    private static final Set<String> WRITABLE_KEY_CACHE = new HashSet<>(100);

    private Keys() {/* no-init */}

    /**
     * A {@link Keys#parse(String) parsed} key that includes information about
     * it's {@link Type} and some other data that may be useful in further
     * processing.
     *
     * @author Jeff Nelson
     */
    public static final class Key {

        /**
         * The {@link Type} of key.
         */
        private final Type type;

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
        private Key(String value, Type type, Object data) {
            this.value = value;
            this.type = type;
            this.data = data;
        }

        /**
         * Return the data associated with this {@link Key}.
         * <p>
         * For example, if this is a {@link Type#NAVIGATION_KEY}, this will
         * return a {@code String[]} containing all of the stops in the path.
         * Or, if this is a {@link Type#FUNCTION_KEY}, this will return the
         * result of {@link Keys#tryParseFunction(String)}.
         * </p>
         * 
         * @return the associated data
         */
        @SuppressWarnings("unchecked")
        public <T> T data() {
            return (T) data;
        }

        /**
         * Return the {@link Type}.
         * 
         * @return the type
         */
        public Type type() {
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

        @Override
        public String toString() {
            return value;
        }

    }

    /**
     * The various kinds of keys that can be {@link Keys#parse(String) parsed}.
     *
     * @author Jeff Nelson
     */
    public static enum Type {
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
