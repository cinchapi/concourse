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
package com.cinchapi.concourse;

/**
 * Thrown to indicate that an operation has no primary or side-effect on its
 * input it's class's state or the state of anything else (e.g., the operation
 * is a no-op).
 *
 * @author Jeff Nelson
 */
public class EmptyOperationException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * Singleton.
     */
    public static EmptyOperationException INSTNACE = new EmptyOperationException();

}
