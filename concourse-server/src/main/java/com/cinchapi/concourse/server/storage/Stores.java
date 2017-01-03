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
package com.cinchapi.concourse.server.storage;

import java.util.regex.Pattern;

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
     * Perform any necessary normalization on {@code operator} so that it can be
     * properly utilized in {@link Store} methods (i.e. convert a utility
     * operator to a functional one).
     * 
     * @param operator
     * @return the normalized Operator
     */
    public static Operator normalizeOperator(Operator operator) {
        switch (operator) {
        case LIKE:
            return Operator.REGEX;
        case NOT_LIKE:
            return Operator.NOT_REGEX;
        case LINKS_TO:
            return Operator.EQUALS;
        default:
            return operator;
        }
    }

    /**
     * Perform any necessary normalization on the {@code value} based on the
     * {@code operator}.
     * 
     * @param operator
     * @param values
     * @return the normalized value
     */
    public static TObject normalizeValue(Operator operator, TObject value) {
        try {
            switch (operator) {
            case REGEX:
            case NOT_REGEX:
                value = Convert.javaToThrift(((String) Convert
                        .thriftToJava(value)).replaceAll(
                        TStrings.REGEX_PERCENT_SIGN_WITHOUT_ESCAPE_CHAR, ".*")
                        .replaceAll(
                                TStrings.REGEX_PERCENT_SIGN_WITH_ESCAPE_CHAR,
                                "%"));
                break;
            case LINKS_TO:
                value = Convert.javaToThrift(Link.to(((Number) Convert
                        .thriftToJava(value)).longValue()));
                break;
            default:
                // noop: default case added to suppress compiler warning
                break;
            }
            return value;
        }
        catch (ClassCastException e) {
            return value;
        }
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
            throw new IllegalArgumentException(Strings.joinWithSpace(key,
                    "is not a valid key"));
        }
        else if(value.isBlank()) {
            throw new IllegalArgumentException("Cannot use a blank value");
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
