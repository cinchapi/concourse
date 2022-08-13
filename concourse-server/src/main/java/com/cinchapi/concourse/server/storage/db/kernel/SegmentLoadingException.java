/*
 * Copyright (c) 2013-2022 Cinchapi Inc.
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
 * An {@link Exception} that indicates an unexpected error occurred when trying
 * to load a {@link Segment}.
 *
 * @author Jeff Nelson
 */
public class SegmentLoadingException extends Exception {

    private static final long serialVersionUID = -6732377307672149738L;

    /**
     * Construct a new instance.
     * 
     * @param clazz
     * @param error
     */
    public SegmentLoadingException(String message) {
        super(message);
    }

    /**
     * Construct a new instance.
     * 
     * @param message
     * @param e
     */
    public SegmentLoadingException(String message, Exception cause) {
        super(message, cause);
    }

}
