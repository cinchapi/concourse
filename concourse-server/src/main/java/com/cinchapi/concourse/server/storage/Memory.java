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
package com.cinchapi.concourse.server.storage;

/**
 * {@link Memory} provides insight into the internal mechanics of a
 * {@link Store}.
 *
 * @author Jeff Nelson
 */
public interface Memory {

    /**
     * Return {@code true} if the {@link Store} has all of its known data for
     * {@code record} loaded.
     * 
     * @param record
     * @return a boolean that indicates if the {@code record} is loaded
     */
    public boolean contains(long record);

    /**
     * Return {@code true} if the {@link Store} has all of its known data for
     * {@code key} in {@code record} loaded.
     * 
     * @param key
     * @param record
     * @return a boolean that indicates if the {@code key}/{@code record} is
     *         loaded
     */
    public boolean contains(String key, long record);

    /**
     * Return {@code true} if the {@link Store} has all of its known data for
     * {@code key} loaded.
     * 
     * @param key
     * @return a boolean that indicates if the {@code key} is loaded
     */
    public boolean contains(String key);

}
