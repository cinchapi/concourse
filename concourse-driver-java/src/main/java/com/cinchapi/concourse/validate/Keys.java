/*
 * Copyright (c) 2013-2019 Cinchapi Inc.
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

/**
 * Utility functions for data keys.
 *
 * @author Jeff Nelson
 */
public final class Keys {

    /**
     * A pre-compiled regex pattern that is used to validate that each key is
     * non-empty, alphanumeric with no special characters other than underscore
     * (_).
     */
    private static final Pattern KEY_VALIDATION_REGEX = Pattern
            .compile("^[a-zA-Z0-9_]+$");

    /**
     * Return {@code true} of the {@code key} is a valid data key for writing.
     * 
     * @param key
     * @return {@code true} if the provided {@code key} is valid for writing
     */
    public static boolean isWritable(String key) {
        return key.length() > 0 && KEY_VALIDATION_REGEX.matcher(key).matches();
    }

    private Keys() {/* no-init */}
}
