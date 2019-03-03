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
package com.cinchapi.concourse.security;

/**
 * An enum that describes the possible user permissions.
 *
 * @author Jeff Nelson
 */
public enum Permission {
    READ, WRITE;

    /**
     * Case insensitive version of {@link Permission#valueOf(String)}.
     * 
     * @param value
     * @return the parsed {@link Permission}
     */
    public static Permission valueOfIgnoreCase(String value) {
        return Permission.valueOf(value.toUpperCase());
    }
}
