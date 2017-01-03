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
package com.cinchapi.concourse.server.http;

import javax.annotation.concurrent.Immutable;

import com.cinchapi.concourse.thrift.AccessToken;

/**
 * A token that identifies an HTTP session and helps to route requests
 * appropriately.
 * <p>
 * An {@link HttpAuthToken} is made up of an {@link AccessToken}, fingerprint
 * and environment.
 * </p>
 * 
 * @author Jeff Nelson
 */
@Immutable
public final class HttpAuthToken {

    /**
     * The {@link AccessToken} component.
     */
    private final AccessToken accessToken;

    /**
     * The environment component.
     */
    private final String environment;

    /**
     * The fingerprint component.
     */
    private final String fingerprint;

    /**
     * Construct a new instance.
     * 
     * @param accessToken
     * @param environment
     * @param fingerprint
     */
    HttpAuthToken(AccessToken accessToken, String environment,
            String fingerprint) {
        this.accessToken = accessToken;
        this.environment = environment;
        this.fingerprint = fingerprint;
    }

    /**
     * Return the {@link AccessToken} associated with this {@link HttpAuthToken
     * auth token}. The AccessToken allows the request to invoke methods
     * defined in Concourse Server.
     * 
     * @return the {@link AccessToken} component
     */
    public AccessToken getAccessToken() {
        return accessToken;
    }

    /**
     * Return the environment associated with this {@link HttpAuthToken auth
     * token}.
     * 
     * @return the environment component
     */
    public String getEnvironment() {
        return environment;
    }

    /**
     * Return the fingerprint associated with this {@link HttpAuthToken auth
     * token}. The fingerprint identifies the precise device from which the
     * request originated.
     * 
     * @return the fingerprint component
     */
    public String getFingerprint() {
        return fingerprint;
    }
}
