/*
 * Copyright (c) 2013-2020 Cinchapi Inc.
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
package com.cinchapi.concourse.server.storage.db;

/**
 * An {@link Exception} that indicates an unexpected error occurred when trying
 * to load a {@link Segment} {@link Block block}.
 *
 * @author Jeff Nelson
 */
class SegmentLoadingException extends Exception {

    private static final long serialVersionUID = -6732377307672149738L;

    /**
     * The type of {@link Block} that experienced the load error.
     */
    private final Class<? extends Block<?, ?, ?>> clazz;

    /**
     * The error.
     */
    private final Exception error;

    /**
     * Construct a new instance.
     * 
     * @param clazz
     * @param error
     */
    public SegmentLoadingException(Class<? extends Block<?, ?, ?>> clazz,
            Exception error) {
        super(error);
        this.clazz = clazz;
        this.error = error;
    }

    /**
     * Return the {@link Block} type that experienced the load error.
     * 
     * @return the {@link Block} {@link Class type}
     */
    public Class<? extends Block<?, ?, ?>> blockType() {
        return clazz;
    }

    /**
     * Return the load error.
     * 
     * @return the load error
     */
    public Exception error() {
        return error;
    }

}
