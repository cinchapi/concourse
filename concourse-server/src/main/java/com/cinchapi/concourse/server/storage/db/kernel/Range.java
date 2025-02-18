/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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
package com.cinchapi.concourse.server.storage.db.kernel;

/**
 * An interface representing a range defined by start and end positions.
 * 
 * This interface encapsulates the concept of a range with distinct start and
 * end points. It can be used to represent any interval or span that is defined
 * by two positions, typically in a linear space such as indices, timestamps, or
 * memory addresses.
 *
 * Implementations of this interface should ensure that the start position is
 * always less than or equal to the end position.
 *
 * @author Jeff Nelson
 */
interface Range {

    /**
     * Return the end position.
     * 
     * @return the end position
     */
    public long end();

    /**
     * Return the start position.
     * 
     * @return the start position
     */
    public long start();

}
