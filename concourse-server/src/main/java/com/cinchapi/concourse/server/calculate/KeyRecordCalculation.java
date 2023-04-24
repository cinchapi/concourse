/*
 * Copyright (c) 2013-2023 Cinchapi Inc.
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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A calculation over all the values stored for a single key in a single record.
 * 
 * @author Jeff Nelson
 */
@FunctionalInterface
public interface KeyRecordCalculation {

    /**
     * Perform the calculation, given the {@code running} result and the latest
     * {@code value} stored for the key in the record or return the latest
     * {@code value} if the {@code running} result is {@code null}.
     * 
     * @param running the current running result
     * @param value the latest value to use in the calculation
     * @return the new running result
     */
    public default Number calculate(@Nullable Number running, Number value) {
        return running != null ? aggregate(running, value) : value;
    }

    /**
     * Aggregate the latest {@code value} into the {@code running} result.
     * 
     * @param running the current running result
     * @param value the latest value to use in the calculation
     * @return the new running result
     */
    public Number aggregate(@Nonnull Number running, Number value);

}
