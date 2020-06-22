/*
 * Copyright (c) 2013-2020 Cinchapi Inc.
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

import java.util.regex.Pattern;

import javax.annotation.Nullable;

import com.cinchapi.ccl.grammar.FunctionKeySymbol;
import com.cinchapi.ccl.grammar.Symbol;
import com.cinchapi.ccl.syntax.AbstractSyntaxTree;
import com.cinchapi.ccl.syntax.FunctionTree;
import com.cinchapi.ccl.type.Function;
import com.cinchapi.ccl.type.function.ImplicitKeyRecordFunction;
import com.cinchapi.concourse.lang.ConcourseCompiler;

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
        return key.length() > 0 && KEY_VALIDATION_REGEX.matcher(key).matches();
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

    private Keys() {/* no-init */}
}
