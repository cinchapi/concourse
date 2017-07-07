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

import javax.annotation.Nullable;

import com.cinchapi.concourse.server.plugin.io.InterProcessCommunication;
import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.thrift.TransactionToken;

/**
 * Marks a thread as being authorized to use the {@link ConcourseRuntime}.
 * 
 * @author Jeff Nelson
 */
public interface ConcourseRuntimeAuthorized {

    /**
     * Return the {@link AccessToken} associated with the user session that owns
     * this thread.
     * 
     * @return the associated {@link AccessToken}
     */
    public AccessToken accessToken();

    /**
     * Return the name of the most recent environment associated with the user
     * session that owns this thread.
     * 
     * @return the environment
     */
    public String environment();

    /**
     * Return the {@link InterProcessCommunication} segment that is used to send
     * any outgoing messages.
     * 
     * @return the request channel
     */
    public InterProcessCommunication outgoing();

    /**
     * Return the most recent {@link TransactionToken} associated with the user
     * session that owns this thread.
     * 
     * @return the {@link TransactionToken}
     */
    @Nullable
    public TransactionToken transactionToken();

    /**
     * Return the map of responses that are sent by the upstream service.
     * 
     * @return the responses
     */
    public ConcurrentMap<AccessToken, RemoteMethodResponse> responses();

}
