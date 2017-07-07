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
package com.cinchapi.concourse.server.storage;

/**
 * An unchecked exception that is thrown when an attempt is made to operate on a
 * closed {@link AtomicOperation}.
 * 
 * @author Jeff Nelson
 */
public class AtomicStateException extends RuntimeException {

    /**
     * Sometimes an {@link AtomicStateException} is used so a caller can signal
     * to itself that it needs to stop an in-process {@link AtomicOperation
     * atomic operation} and retry, starting over from scratch. This is a
     * constant exception object that can be reused to avoid the cost of
     * creating a new one merely to signal a state change.
     */
    public static final AtomicStateException RETRY = new AtomicStateException();

    private static final long serialVersionUID = 1L;

}
