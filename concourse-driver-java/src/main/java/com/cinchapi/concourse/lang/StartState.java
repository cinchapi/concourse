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
 * The {@link StartState} marks the logical beginning of a new {@link Criteria}.
 * 
 * @author Jeff Nelson
 */
public class StartState extends State {

    /**
     * Construct a new instance.
     * 
     * @param criteria
     */
    public StartState(Criteria criteria) {
        super(criteria);
    }

    /**
     * Add a sub {@code criteria} to the Criteria that is building. A sub
     * criteria is one that is wrapped in parenthesis.
     * 
     * @param criteria
     * @return the builder
     */
    public BuildableStartState group(Criteria criteria) {
        this.criteria.add(criteria);
        return new BuildableStartState(this.criteria);
    }

    /**
     * Add a sub {@code criteria} to the Criteria that is building. A sub
     * criteria is one that is wrapped in parenthesis.
     * 
     * @param criteria
     * @return the builder
     */
    // CON-131: account for cases when the caller forgets to "build" the
    // sub criteria
    public BuildableStartState group(Object criteria) {
        if(criteria instanceof BuildableState) {
            return group(((BuildableState) criteria).build());
        }
        else {
            throw new IllegalArgumentException(criteria
                    + " is not a valid argument for the group method");
        }
    }

    /**
     * Add a {@code key} to the Criteria that is building.
     * 
     * @param key
     * @return the builder
     */
    public KeyState key(String key) {
        criteria.add(KeySymbol.create(key));
        return new KeyState(criteria);
    }

}
