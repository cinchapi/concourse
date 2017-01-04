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
package com.cinchapi.concourse.server.plugin;

import java.util.concurrent.ConcurrentMap;

import com.cinchapi.concourse.server.plugin.io.SharedMemory;
import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.thrift.TransactionToken;
import com.google.common.annotations.VisibleForTesting;

/**
 * A {@link BackgroundThread} can be used by a {@link Plugin} or its supporting
 * classes to make non-user (e.g. service) based requests to the upstream
 * {@link ConcourseRuntime concourse runtime}.
 * <p>
 * Recall that, by using a {@link RemoteInvocationThread} allows a plugin and
 * its supporting classes to seamlessly perform server-side actions on behalf of
 * a user as long as the execution context originates from a user request
 * proxied from Concourse Server to the plugin.
 * </p>
 * <p>
 * In order to background tasks to make requests to the upstream service
 * (outside of a user request context), a {@link BackgroundThread} must be used
 * in order to simulate the effects of using a {@link RemoteInvocationThread}.
 * </p>
 * 
 * @author Jeff Nelson
 */
public class BackgroundThread extends Thread implements
        ConcourseRuntimeAuthorized {

    /**
     * The {@link AccessToken} to use when making non-user requests via
     * {@link ConcourseRuntime}.
     */
    @VisibleForTesting
    protected static AccessToken SERVICE_TOKEN = Plugin.SERVICE_TOKEN;

    /**
     * The current environment of the runtime which which this thread interacts.
     */
    private String environment;

    /**
     * The {@link SharedMemory} channel to use when sending messages to the
     * upstream service.
     */
    private final SharedMemory outgoing;

    /**
     * The collection of responses sent from the upstream service.
     */
    private final ConcurrentMap<AccessToken, RemoteMethodResponse> responses;

    /**
     * Construct a new instance.
     * 
     * @param runnable
     * @param environment
     * @param outgoing
     * @param responses
     */
    public BackgroundThread(Runnable runnable, String environment,
            SharedMemory outgoing,
            ConcurrentMap<AccessToken, RemoteMethodResponse> responses) {
        super(runnable);
        this.environment = environment;
        this.outgoing = outgoing;
        this.responses = responses;
        setDaemon(true);
    }

    @Override
    public AccessToken accessToken() {
        return SERVICE_TOKEN;
    }

    @Override
    public String environment() {
        return environment;
    }

    /**
     * Set the environment to use when making requests to the upstream service.
     * 
     * @param environment the environment to use for upstream requests
     */
    public void environment(String environment) {
        this.environment = environment;
    }

    @Override
    public SharedMemory outgoing() {
        return outgoing;
    }

    @Override
    public ConcurrentMap<AccessToken, RemoteMethodResponse> responses() {
        return responses;
    }

    @Override
    public final TransactionToken transactionToken() {
        // NOTE: BackgroundThreads are not allowed to participate in
        // transactions.
        return null;
    }

}
