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
package com.cinchapi.concourse.lang;

import com.cinchapi.ccl.Compiler;
import com.cinchapi.ccl.syntax.AbstractSyntaxTree;
import com.cinchapi.concourse.thrift.TCriteria;
import com.cinchapi.concourse.util.Convert;
import com.google.common.collect.Multimap;

/**
 * A CCL {@link Compiler} for Concourse.
 *
 * @author Jeff Nelson
 */
public final class ConcourseCompiler extends Compiler {

    /**
     * Return a {@link ConcourseCompiler}.
     * 
     * @return a {@link ConcourseCompiler}
     */
    public static ConcourseCompiler get() {
        return SINGLETON;
    }

    /**
     * Singleton instance of {@link ConcourseCompiler}.
     */
    private static ConcourseCompiler SINGLETON = new ConcourseCompiler();

    /**
     * The delegate that does the work.
     */
    private Compiler delegate;

    /**
     * Construct a new instance.
     */
    private ConcourseCompiler() {
        super(Convert::stringToJava, Convert::stringToOperator);
        this.delegate = ConcourseCompiler.create(Convert::stringToJava,
                Convert::stringToOperator);
    }

    /**
     * {@link #parse(String) Parse} the {@code criteria}.
     * 
     * @param criteria
     * @return an {@link AbstractSyntaxTree} representing the {@code criteria}
     */
    public final AbstractSyntaxTree parse(Criteria criteria) {
        return parse(criteria.ccl());
    }

    /**
     * {@link #parse(String) Parse} the {@code criteria} and bind the local
     * {@code data}.
     * 
     * @param criteria
     * @return an {@link AbstractSyntaxTree} representing the {@code criteria}
     */
    public final AbstractSyntaxTree parse(Criteria criteria,
            Multimap<String, Object> data) {
        return parse(criteria.ccl(), data);
    }

    @Override
    public AbstractSyntaxTree parse(String ccl, Multimap<String, Object> data) {
        return delegate.parse(ccl, data);
    }

    /**
     * {@link #parse(String) Parse} the {@code criteria}.
     * 
     * @param criteria
     * @return an {@link AbstractSyntaxTree} representing the {@code criteria}
     */
    public final AbstractSyntaxTree parse(TCriteria criteria) {
        return parse(Language.translateFromThriftCriteria(criteria).ccl());
    }

    /**
     * {@link #parse(String) Parse} the {@code criteria} and bind the local
     * {@code data}.
     * 
     * @param criteria
     * @return an {@link AbstractSyntaxTree} representing the {@code criteria}
     */
    public final AbstractSyntaxTree parse(TCriteria criteria,
            Multimap<String, Object> data) {
        return parse(Language.translateFromThriftCriteria(criteria).ccl(),
                data);
    }

}
