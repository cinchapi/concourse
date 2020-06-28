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
package com.cinchapi.concourse.util;

/**
 * Utilities for link-navigation queries.
 * 
 * @author Jeff Nelson
 */
public final class Navigation {

    /**
     * Given a navigation scheme, return the destination to which the scheme
     * navigates.
     * 
     * @param scheme
     * @return the destination
     */
    public static String getKeyDestination(String scheme) {
        String[] toks = scheme.split("\\.");
        return toks[toks.length - 1];
    }

    private Navigation() {/* no-op */}

}
