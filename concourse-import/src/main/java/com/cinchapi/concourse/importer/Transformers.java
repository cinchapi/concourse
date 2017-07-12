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
package com.cinchapi.concourse.importer;

import java.util.Arrays;

import com.cinchapi.concourse.importer.util.Importables;
import com.cinchapi.concourse.util.KeyValue;
import com.cinchapi.concourse.util.Strings;
import com.google.common.base.CaseFormat;

/**
 * Utilities for import {@link Transformer} objects.
 * 
 * @author Jeff Nelson
 */
public final class Transformers {

    /**
     * A {@link Transformer} that strips invalid characters from the key. Some
     * invalid characters are replaced with valid stand-ins.
     */
    private final static Transformer KEY_STRIP_INVALID_CHARS = (key, value) -> {
        StringBuilder sb = new StringBuilder();
        for (char c : key.toCharArray()) {
            if(Importables.isValidKeyCharacter(c)) {
                sb.append(c);
            }
            else if(c == '#') {
                sb.append("num");
            }
            else if(c == ' ') {
                sb.append("_");
            }
        }
        return new KeyValue<>(sb.toString(), value);
    };

    /**
     * A {@link Transformer} that will strip single and double quotes from the
     * beginning and end of both the key and value.
     */
    private final static Transformer STRIP_QUOTES = (key, value) -> {
        if(Strings.isWithinQuotes(key)) {
            key = key.substring(1, key.length() - 1);
        }
        if(Strings.isWithinQuotes(value)) {
            value = value.substring(1, value.length() - 1);
        }
        return new KeyValue<>(key, value);
    };

    private Transformers() {/* no init */}

    /**
     * Return a {@link CompositeTransformer} that invokes each of the
     * {@code transformers} in order.
     * 
     * @param transformers the {@link Transformer transformers} to invoke
     * @return a {@link CompositeTransformer}
     */
    public static Transformer compose(Transformer... transformers) {
        return new CompositeTransformer(Arrays.asList(transformers));
    }

    /**
     * Return a {@link Transformer} that converts keys {@code from} one case
     * format {@code to} another one.
     * 
     * @param from the original {@link CaseFormat}
     * @param to the desired {@link CaseFormat}
     * @return the {@link Transformer}
     */
    public static Transformer keyCaseFormat(CaseFormat from, CaseFormat to) {
        return (key, value) -> {
            key = from.to(to, key);
            return new KeyValue<>(key, value);
        };
    }

    /**
     * Return a {@link Transformer} that converts all the characters in a key to
     * lowercase. No other modifications to the key are made.
     * 
     * @return the {@link Transformer}
     */
    public static Transformer keyToLowerCase() {
        return (key, value) -> new KeyValue<>(key.toLowerCase(), value);
    }

    /**
     * Return a {@link Transformer} that strips invalid characters from the key.
     * Some of those characters are replaced with valid stand-ins.
     * <h1>Replacements</h1>
     * <ul>
     * <li>a white space character (e.g. ' ') is replaced with an
     * underscore</li>
     * <li>the pound sign is replaced the string {@code num}</li>
     * </ul>
     * <p>
     * 
     * @return the {@link Transformer}
     */
    public static Transformer keyStripInvalidChars() {
        return KEY_STRIP_INVALID_CHARS;
    }

    /**
     * Return a {@link Transformer} that will strip single and double quotes
     * from the beginning and end of both the key and value.
     */
    public static Transformer stripQuotes() {
        return STRIP_QUOTES;
    }

}
