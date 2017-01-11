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
package com.cinchapi.concourse.importer;

import javax.annotation.Nullable;

import com.cinchapi.concourse.util.KeyValue;

/**
 * A {@link Transformer} is a routine that takes a key/value pair and
 * potentially alters one or both of them, prior to import.
 * <p>
 * Sometimes, raw data from a source must be modified before being imported into
 * Concourse, for example:
 * <ul>
 * <li>Modifying keys by changing their case or stripping illegal characters</li>
 * <li>Normalizing values, for example, converting strings to a specific case or
 * sanitizing</li>
 * <li>Compacting representation by values from an enumerated set of values to a
 * simple integer</li>
 * <li>Constructing a link or resolvable link</li>
 * </ul>
 * 
 * @author Jeff Nelson
 */
@FunctionalInterface
public interface Transformer {

    /**
     * Potentially transform one or both of the {@code key}/{@code value} pair.
     * If no transformation should occur, it is acceptable to return
     * {@code null} to inform the caller that the import values are fine.
     * Otherwise, the preferred pair should be wrapped in a {@link KeyValue}
     * object.
     * 
     * @param key the raw key to potentially transform
     * @param value the raw value to potentially transform; in which case, it is
     *            acceptable to return any kind of object, but it is recommended
     *            to return an encoded String (i.e. don't return a
     *            {@link com.cinchapi.concourse.Link} object, but return a
     *            string that encodes a link (@record@) instead)
     * @return a {@link KeyValue} object that contains the transformed
     *         {@code key}/{@code value} pair or {@code null} if no
     *         transformation occurred
     */
    @Nullable
    public KeyValue<String, Object> transform(String key, String value);

}
