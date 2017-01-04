/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse.shell;

/**
 * This is the base class from all "throwables" that indicate an attempt by
 * {@link ConcourseShell} to evaluate input yielded an irregular result. In most
 * cases, ConcourseShell will return a result from Concourse or one of its
 * built-in functions; however, there are some cases when the input causes
 * ConcourseShell to do something different (i.e. display the HELP text when the
 * user enters "help").
 * 
 * @author Jeff Nelson
 */
@SuppressWarnings("serial")
abstract class IrregularEvaluationResult extends Throwable {

    /**
     * Construct a new instance.
     * 
     * @param message
     */
    public IrregularEvaluationResult(String message) {
        super(message);
    }
}
