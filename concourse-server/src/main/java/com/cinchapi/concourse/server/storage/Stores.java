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
package com.cinchapi.concourse.server.storage;

import java.util.regex.Pattern;

import javax.annotation.concurrent.Immutable;

import com.cinchapi.concourse.Link;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.Strings;
import com.cinchapi.concourse.util.TStrings;

/**
 * {@link Store} based utility functions.
 * 
 * @author Jeff Nelson
 */
public final class Stores {

    /**
     * Perform any necessary normalization on the {@code value} based on the
     * {@code operator}.
     * 
     * @param operator
     * @param values
     * @return the operationalized parameters
     */
    public static OperationParameters operationalize(Operator operator,
            TObject... values) {
        // Transform the operator to its functional alias, given the
        // transformations that will be made to the value(s).
        Operator original = operator;
        switch (operator) {
        case LIKE:
            operator = Operator.REGEX;
            break;
        case NOT_LIKE:
            operator = Operator.NOT_REGEX;
            break;
        case LINKS_TO:
            operator = Operator.EQUALS;
            break;
        default:
            break;
        }

        // Transform the values based on the original operator, if necessary.
        TObject[] ovalues = new TObject[values.length];
        for (int i = 0; i < ovalues.length; ++i) {
            TObject value = values[i];
            try {
                switch (original) {
                case REGEX:
                case NOT_REGEX:
                case LIKE:
                case NOT_LIKE:
                    value = Convert.javaToThrift(
                            ((String) Convert.thriftToJava(value)).replaceAll(
                                    TStrings.REGEX_PERCENT_SIGN_WITHOUT_ESCAPE_CHAR,
                                    ".*").replaceAll(
                                            TStrings.REGEX_PERCENT_SIGN_WITH_ESCAPE_CHAR,
                                            "%"));
                    break;
                case LINKS_TO:
                    value = Convert.javaToThrift(
                            Link.to(((Number) Convert.thriftToJava(value))
                                    .longValue()));
                    break;
                default:
                    break;
                }

            }
            catch (ClassCastException e) {/* ignore */}
            ovalues[i] = value;
        }

        return new OperationParameters(operator, ovalues);
    }

    /**
     * Perform validation on the {@code key} and {@code value} and throw an
     * exception if necessary.
     * 
     * @param key
     * @param value
     */
    public static void validateWriteData(String key, TObject value) { // CON-21
        if(key.length() == 0 || !KEY_VALIDATION_REGEX.matcher(key).matches()) {
            throw new IllegalArgumentException(
                    Strings.joinWithSpace(key, "is not a valid key"));
        }
        else if(value.isBlank()) {
            throw new IllegalArgumentException("Cannot use a blank value");
        }
    }

    /**
     * A container class that holds operational parameters that can be passed to
     * various {@link Store} methods.
     * 
     * @author Jeff Nelson
     */
    @Immutable
    public static final class OperationParameters {

        private final Operator operator;
        private final TObject[] values;

        /**
         * @param operator
         * @param values
         */
        private OperationParameters(Operator operator, TObject[] values) {
            this.operator = operator;
            this.values = values;
        }

        /**
         * Return the {@link #operator}.
         * 
         * @return the operator
         */
        public Operator operator() {
            return operator;
        }

        /**
         * Return the {@link #values}.
         * 
         * @return the values
         */
        public TObject[] values() {
            return values;
        }
    }

    /**
     * A pre-compiled regex pattern that is used to validate that each key is
     * non-empty, alphanumeric with no special characters other than underscore
     * (_).
     */
    private static final Pattern KEY_VALIDATION_REGEX = Pattern
            .compile("^[a-zA-Z0-9_]+$");
}
