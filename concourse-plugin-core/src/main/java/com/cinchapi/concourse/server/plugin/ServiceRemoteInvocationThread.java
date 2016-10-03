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
package com.cinchapi.concourse.server.plugin;

import java.util.concurrent.ConcurrentMap;

import com.cinchapi.concourse.server.plugin.io.SharedMemory;
import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.thrift.TransactionToken;
import com.google.common.annotations.VisibleForTesting;

/**
 * A {@link RemoteInvocationThread} for non-user (e.g. service) based requests.
 * 
 * @author Jeff Nelson
 */
public class ServiceRemoteInvocationThread extends Thread implements
        ConcourseRuntimeAuthorized {

    @VisibleForTesting
    protected static AccessToken SERVICE_TOKEN = Plugin.SERVICE_TOKEN;

    private String environment;
    private final ConcurrentMap<AccessToken, RemoteMethodResponse> responses;
    private final SharedMemory outgoing;

    public ServiceRemoteInvocationThread(Runnable runnable, String environment,
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

    public void environment(String environment) {
        this.environment = environment;
    }

    @Override
    public SharedMemory outgoing() {
        return outgoing;
    }

    @Override
    public final TransactionToken transactionToken() {
        return null;
    }

    @Override
    public ConcurrentMap<AccessToken, RemoteMethodResponse> responses() {
        return responses;
    }

}
