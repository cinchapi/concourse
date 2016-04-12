/*
 * Copyright (c) 2013-2016 Cinchapi Inc.
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
package jsr166e;

/**
 * Exception thrown when an error or other exception is encountered
 * in the course of completing a result or task.
 * 
 * @since 1.8
 * @author Doug Lea
 */
public class CompletionException extends RuntimeException {
    private static final long serialVersionUID = 7830266012832686185L;

    /**
     * Constructs a {@code CompletionException} with no detail message.
     * The cause is not initialized, and may subsequently be
     * initialized by a call to {@link #initCause(Throwable) initCause}.
     */
    protected CompletionException() {}

    /**
     * Constructs a {@code CompletionException} with the specified detail
     * message. The cause is not initialized, and may subsequently be
     * initialized by a call to {@link #initCause(Throwable) initCause}.
     * 
     * @param message the detail message
     */
    protected CompletionException(String message) {
        super(message);
    }

    /**
     * Constructs a {@code CompletionException} with the specified detail
     * message and cause.
     * 
     * @param message the detail message
     * @param cause the cause (which is saved for later retrieval by the
     *            {@link #getCause()} method)
     */
    public CompletionException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a {@code CompletionException} with the specified cause.
     * The detail message is set to {@code (cause == null ? null :
     * cause.toString())} (which typically contains the class and
     * detail message of {@code cause}).
     * 
     * @param cause the cause (which is saved for later retrieval by the
     *            {@link #getCause()} method)
     */
    public CompletionException(Throwable cause) {
        super(cause);
    }
}
