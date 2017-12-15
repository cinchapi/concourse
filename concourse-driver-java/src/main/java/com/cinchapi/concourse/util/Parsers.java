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
package com.cinchapi.concourse.util;

import com.cinchapi.ccl.Parser;
import com.cinchapi.concourse.server.GlobalState;
import com.google.common.collect.Multimap;

/**
 * Utilities for {@link Parser}s.
 *
 * @author Jeff Nelson
 */
public final class Parsers {

    /**
     * Return a {@link Parser} for the {@code ccl} statement.
     * 
     * @param ccl a CCL statement
     * @return a {@link Parser}
     */
    public static Parser create(String ccl) {
        return Parser.newParser(ccl,
                GlobalState.PARSER_TRANSFORM_VALUE_FUNCTION,
                GlobalState.PARSER_TRANSFORM_OPERATOR_FUNCTION);
    }

    /**
     * Return a {@link Parser} for the {@code ccl} statement that uses the
     * provided {@code data} for local resolution.
     * 
     * @param ccl a CCL statement
     * @param data a dataset
     * @return a {@link Parser}
     */
    public static Parser create(String ccl, Multimap<String, Object> data) {
        return Parser.newParser(ccl, data,
                GlobalState.PARSER_TRANSFORM_VALUE_FUNCTION,
                GlobalState.PARSER_TRANSFORM_OPERATOR_FUNCTION);
    }

    private Parsers() {/* no-init */}

}
