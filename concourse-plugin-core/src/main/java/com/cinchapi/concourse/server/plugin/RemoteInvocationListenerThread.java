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

import java.nio.ByteBuffer;
import java.util.Set;

import javax.annotation.Nullable;

import com.cinchapi.concourse.server.plugin.io.SharedMemory;
import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.thrift.TransactionToken;
import com.cinchapi.concourse.util.ByteBuffers;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.Reflection;
import com.cinchapi.concourse.util.Serializables;

/**
 * A daemon {@link Thread} that is dedicated to reading
 * {@link RemoteMethodInvocation} messages from a {@link SharedMemory} segment
 * and handling appropriately.
 * 
 * @author Jeff Nelson
 */
final class RemoteInvocationListenerThread extends Thread {

    /**
     * A view of a plugin's clients. This is used so that this thread can detect
     * when it can terminate.
     */
    private final Set<AccessToken> clients;

    /**
     * The credentials for the user session that owns this thread.
     */
    private final AccessToken creds;

    /**
     * The most recent environment associated with the user session.
     */
    private String environment;

    /**
     * The {@link SharedMemory} segment where incoming invocation messages are
     * placed.
     */
    private final SharedMemory incoming;

    /**
     * The local object that contains the methods to invoke.
     */
    private final Object invocationSource;

    /**
     * The {@link SharedMemory} segment that is used for any outgoing invocation
     * messages.
     */
    private final SharedMemory outgoing;

    /**
     * The most recent {@link TransactionToken} associated with the user
     * session.
     */
    @Nullable
    private TransactionToken transaction;

    /**
     * A flag that indicates whether thrift arguments should be passed when
     * invoking a local method on behalf of a remote request.
     */
    private final boolean useLocalThriftArgs;

    /**
     * Construct a new instance.
     * 
     * @param invocationSource
     * @param clients
     * @param incoming
     * @param outgoing
     * @param creds
     * @param useLocalThriftArgs
     */
    public RemoteInvocationListenerThread(Object invocationSource,
            Set<AccessToken> clients, SharedMemory incoming,
            SharedMemory outgoing, AccessToken creds, boolean useLocalThriftArgs) {
        this.invocationSource = invocationSource;
        this.creds = creds;
        this.incoming = incoming;
        this.outgoing = outgoing;
        this.clients = clients;
        this.useLocalThriftArgs = useLocalThriftArgs;
        setDaemon(true);
    }

    /**
     * Return the {@link AccessToken} associated with the user session that owns
     * this thread.
     * 
     * @return the associated {@link AccessToken}
     */
    public AccessToken getAccessToken() {
        return creds;
    }

    /**
     * Return the name of the most recent environment associated with the user
     * session that owns this thread.
     * 
     * @return the environment
     */
    public String getEnvironment() {
        return environment;
    }

    /**
     * Return the {@link SharedMemory} segment that is used to send any outgoing
     * {@link RemoteMethodInvocation} requests.
     * 
     * @return the outgoing channel
     */
    public SharedMemory getOutgoingChannel() {
        return outgoing;
    }

    /**
     * Return the most recent {@link TransactionToken} associated with the user
     * session that owns this thread.
     * 
     * @return the {@link TransactionToken}
     */
    @Nullable
    public TransactionToken getTransactionToken() {
        return transaction;
    }

    @Override
    public final void run() {
        while (clients.contains(creds)) {
            ByteBuffer data = incoming.read();
            RemoteMethodInvocation request = Serializables.read(
                    ByteBuffers.rewind(data), RemoteMethodInvocation.class);
            assert request.creds.equals(creds);
            int argCount = request.args.size() + (useLocalThriftArgs ? 3 : 0);
            Object[] jargs = new Object[argCount];
            int i = 0;
            for (; i < request.args.size(); ++i) {
                jargs[i] = Convert.thriftToJava(request.args.get(i));
            }
            if(useLocalThriftArgs) {
                jargs[i + 1] = request.creds;
                jargs[i + 2] = request.transaction;
                jargs[i + 3] = request.environment;
            }
            transaction = request.transaction;
            environment = request.environment;
            Object result = Reflection.callIfAccessible(invocationSource,
                    request.method, jargs);
            TObject tresult = Convert.javaToThrift(result);
            RemoteMethodResponse response = new RemoteMethodResponse(tresult);
            incoming.respond(Serializables.getBytes(response));
        }
    }

}
