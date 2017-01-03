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
package com.cinchapi.concourse.shell;

/**
 * A {@link Throwable} that is used to signal to the program that the evaluated
 * inputed merits the displaying of HELP text.
 * 
 * @author Jeff Nelson
 */
@SuppressWarnings("serial")
public class HelpRequest extends IrregularEvaluationResult {

    /**
     * The topic for which help is requested.
     */
    public final String topic;

    /**
     * Construct a new instance.
     */
    public HelpRequest() {
        super("");
        this.topic = "";
    }

    /**
     * Construct a new instance.
     * 
     * @param topic
     */
    public HelpRequest(String topic) {
        super(topic);
        this.topic = topic;
    }

}
