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
 * The base class for a language state that can be transformed into a complete
 * and well-formed {@link Criteria}.
 * 
 * @author Jeff Nelson
 */
public abstract class BuildableState extends State {

    /**
     * Construct a new instance.
     * 
     * @param criteria
     */
    protected BuildableState(Criteria criteria) {
        super(criteria);
    }

    /**
     * Build and return the {@link Criteria}.
     * 
     * @return the built Criteria
     */
    public final Criteria build() {
        criteria.close();
        return criteria;
    }

    /**
     * Build a conjunctive clause onto the {@link Criteria} that is building.
     * 
     * @return the builder
     */
    public StartState and() {
        criteria.add(ConjunctionSymbol.AND);
        return new StartState(criteria);
    }

    /**
     * Build a disjunctive clause onto the {@link Criteria} that is building.
     * 
     * @return the builder
     */
    public StartState or() {
        criteria.add(ConjunctionSymbol.OR);
        return new StartState(criteria);
    }

}
