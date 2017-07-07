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
package com.cinchapi.concourse.server.http.errors;

/**
 * An exception that is used to indicate an HTTP Error.
 * 
 * @author Jeff Nelson
 */
public abstract class HttpError extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * The status code to return to the user.
     */
    private final int code;

    /**
     * Construct a new instance.
     * 
     * @param code
     * @param message
     */
    public HttpError(int code, String message) {
        super(message);
        this.code = code;
    }

    /**
     * Return the HTTP error code.
     * 
     * @return the error code
     */
    public int getCode() {
        return this.code;
    }

}
