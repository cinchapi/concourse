/*
 * Copyright (c) 2013-2021 Cinchapi Inc.
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
 * A collection of commonly used regex patterns
 *
 * @author Jeff Nelson
 */
public final class RegexPatterns {

    /**
     * Matches the percent sign without escape character[\%].
     */
    public static final String PERCENT_SIGN_WITH_ESCAPE_CHAR = "\\\\%";

    /**
     * Matches the percent sign without escape character[%].
     */
    public static final String PERCENT_SIGN_WITHOUT_ESCAPE_CHAR = "(?<!\\\\)%";

    private RegexPatterns() {/* no-init */}

}
