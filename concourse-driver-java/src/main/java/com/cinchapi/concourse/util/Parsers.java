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
package com.cinchapi.concourse.util;

import com.cinchapi.ccl.Parser;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.Language;
import com.cinchapi.concourse.thrift.Operators;
import com.cinchapi.concourse.thrift.TCriteria;
import com.google.common.collect.Multimap;

/**
 * Utilities for {@link Parser}s.
 *
 * @author Jeff Nelson
 */
public final class Parsers {

    /**
     * Return a {@link Parser} for the {@code criteria}.
     * 
     * @param criteria
     * @return a {@link Parser}
     */
    public static Parser create(Criteria criteria) {
        return create(criteria.getCclString());
    }

    /**
     * Return a {@link Parser} for the {@code criteria} that uses the provided
     * {@code data} for local resolution.
     * 
     * @param criteria
     * @param data a dataset
     * @return a {@link Parser}
     */
    public static Parser create(Criteria criteria,
            Multimap<String, Object> data) {
        return create(criteria.getCclString(), data);
    }

    /**
     * Return a {@link Parser} for the {@code ccl} statement.
     * 
     * @param ccl a CCL statement
     * @return a {@link Parser}
     */
    public static Parser create(String ccl) {
        return Parser.create(ccl, Convert::stringToJava,
                Convert::stringToOperator, Operators::evaluate);
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
        return Parser.create(ccl, data, Convert::stringToJava,
                Convert::stringToOperator, Operators::evaluate);
    }

    /**
     * Return a {@link Parser} for the {@code criteria}.
     * 
     * @param criteria
     * @return a {@link Parser}
     */
    public static Parser create(TCriteria criteria) {
        return create(
                Language.translateFromThriftCriteria(criteria).getCclString());
    }

    /**
     * Return a {@link Parser} for the {@code criteria} that uses the provided
     * {@code data} for local resolution.
     * 
     * @param criteria
     * @param data a dataset
     * @return a {@link Parser}
     */
    public static Parser create(TCriteria criteria,
            Multimap<String, Object> data) {
        return create(
                Language.translateFromThriftCriteria(criteria).getCclString(),
                data);
    }

    private Parsers() {/* no-init */}

}
