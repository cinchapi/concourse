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
package com.cinchapi.concourse.lang;

import com.cinchapi.concourse.Timestamp;

/**
 * The {@link State} that expects the current token to be the last or the next
 * token to be a value or conjunction specification.
 * 
 * @author Jeff Nelson
 */
public class ValueState extends BuildableState {

    /**
     * Construct a new instance.
     * 
     * @param criteria
     */
    protected ValueState(Criteria criteria) {
        super(criteria);
    }

    /**
     * Add the specified {@code value} to the {@link Criteria} that is building.
     * 
     * @param value
     * @return the builder
     */
    public ValueState value(Object value) {
        criteria.add(ValueSymbol.create(value));
        return new ValueState(criteria);
    }
    
    /**
     * Add the specified {@code timestamp} to the {@link Criteria} that is building.
     * @param timestamp
     * @return the builder
     */
    public TimestampState at(Timestamp timestamp){
        criteria.add(TimestampSymbol.create(timestamp));
        return new TimestampState(criteria);
    }

}
