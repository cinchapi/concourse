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

import com.cinchapi.concourse.annotate.PackagePrivate;
import com.cinchapi.concourse.server.plugin.io.SharedMemory;
import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.util.ByteBuffers;
import com.cinchapi.concourse.util.Serializables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * A {@link Plugin} contains methods that extend the functionality of
 * {@link com.cinchapi.concourse.Concourse Concourse}. Plugin methods can be
 * dynamically accessed using the
 * {@link com.cinchapi.concourse.Concourse#invokePlugin(String, String, Object...)
 * invokePlugin} method.
 * 
 * @author Jeff Nelson
 */
public abstract class Plugin {

    /**
     * A reference to the local Concourse Server {@link ConcourseRuntime
     * runtime} to which this plugin is registered.
     */
    protected final ConcourseRuntime runtime;

    /**
     * The main channel of communication between Concourse and this
     * {@link Plugin}. Concourse establishes this channel when creating the
     * plugin and uses it to send high-level commands. Separate channels are
     * used for RPC communication between Concourse and the plugin.
     */
    private final SharedMemory broadcast;

    /**
     * The {@link AccessToken user sessions} that have interacted with this
     * plugin during its lifetime.
     */
    private final Set<AccessToken> clients = Sets.newSetFromMap(Maps
            .<AccessToken, Boolean> newConcurrentMap());

    /**
     * An object passed in from the plugin's ad-hoc main method parent to
     * coordinate communication about when the plugin is instructed to stop and
     * the housing JVM should terminate.
     */
    private final Object notifier;

    /**
     * Construct a new instance.
     * 
     * @param broadcastStation the location where the main line of communication
     *            between
     *            Concourse and the plugin occurs
     * @param notifier an object that the plugin uses to notify of shutdown
     */
    public Plugin(String broadcastStation, Object notifier) {
        this.runtime = ConcourseRuntime.getRuntime();
        this.broadcast = new SharedMemory(broadcastStation);
        this.notifier = notifier;
    }

    /**
     * Start the plugin and process requests until told to {@link #stop()}.
     */
    public void run() {
        ByteBuffer data;
        while ((data = broadcast.read()) != null) {
            Instruction type = ByteBuffers.getEnum(data, Instruction.class);
            ByteBuffer message = ByteBuffers.getRemaining(data);
            if(type == Instruction.CONNECT_CLIENT) {
                PluginClient client = Serializables.read(message,
                        PluginClient.class);
                Object invocationSource = this;
                SharedMemory incoming = new SharedMemory(client.inbox);
                SharedMemory outgoing = new SharedMemory(client.outbox);
                boolean useLocalThriftArgs = false;

                // Create a thread that listen for messages from the server
                // requesting the invocation of a local method.
                RemoteInvocationListenerThread listener = new RemoteInvocationListenerThread(
                        invocationSource, clients, incoming, outgoing,
                        client.creds, useLocalThriftArgs);
                clients.add(client.creds);

                // Start the listener and let it run until the client
                // disconnects or the plugin is shutdown
                listener.start();
                broadcast.respond(ByteBuffers.nullByteBuffer());
            }
            else if(type == Instruction.DISCONNECT_CLIENT) {
                // TODO get the access token and do client.remove(creds);
            }
            else if(type == Instruction.STOP) {
                stop();
                break;
            }
        }
    }

    /**
     * Stop the plugin immediately.
     */
    public void stop() {
        clients.clear();
        notifier.notify();
    }

    /**
     * High level instructions that are communicated from Concourse Server to
     * the plugin via {@link #broadcast} channel.
     * 
     * @author Jeff Nelson
     */
    @PackagePrivate
    enum Instruction {
        CONNECT_CLIENT, DISCONNECT_CLIENT, STOP
    }

}
