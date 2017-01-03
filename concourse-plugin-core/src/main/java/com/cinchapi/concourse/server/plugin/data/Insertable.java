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
package com.cinchapi.concourse.server.plugin.data;

/**
 * <p>
 * An interface implemented by the {@link Dataset} that limits transactions to a
 * write-only (specifically insertion) basis. This prevents data leakage and
 * protects against malicious or uninformed usage.
 * </p>
 * 
 * <p>
 * The interface should be parameterized in the same way as the implementing
 * Dataset.
 * </p>
 *
 * @see {@link Dataset}
 * @param <E> entity
 * @param <A> attribute
 * @param <V> value
 */
public interface Insertable<E, A, V> {

    /**
     * Add an association between {@code attribute} and {@code value} within the
     * {@code entity}.
     * 
     * @param entity the entity
     * @param attribute the attribute
     * @param value the value
     * @return {@code true} if the association can be added because it didn't
     *         previously exist
     */
    public boolean insert(E entity, A attribute, V value);

}
