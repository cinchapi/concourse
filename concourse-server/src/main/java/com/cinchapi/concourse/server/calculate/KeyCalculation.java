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
package com.cinchapi.concourse.server.calculate;

import java.util.Set;

/**
 * A calculation across an entire field.
 * 
 * @author Jeff Nelson
 */
@FunctionalInterface
public interface KeyCalculation {

    /**
     * Perform the calculation, given the {@code running} result and the latest
     * {@code value}, which is stored in each of the {@code records}.
     * 
     * @param running the current running result
     * @param value the value to use for the calculation
     * @param records all the records in which the {@code value} is stored
     * @return the new running result
     */
    public Number calculate(Number running, Number value, Set<Long> records);

}
