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
package com.cinchapi.concourse.lang;

/**
 * A {@link StartState} that is also a {@link BuildableState}. This distinction
 * is important so that we don't allow the caller to build an empty criteria or
 * a criteria with a hanging conjunction (because those conditions would simply
 * place the builder in a {@link StateState} which is un-buildable).
 * 
 * @author Jeff Nelson
 */
public class BuildableStartState extends BuildableState {

    /**
     * Construct a new instance.
     * 
     * @param criteria
     */
    protected BuildableStartState(Criteria criteria) {
        super(criteria);
    }

}
