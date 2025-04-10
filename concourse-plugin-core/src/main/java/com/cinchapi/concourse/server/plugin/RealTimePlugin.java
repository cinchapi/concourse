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
package com.cinchapi.concourse.server.plugin;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.annotate.PackagePrivate;
import com.cinchapi.concourse.server.plugin.io.InterProcessCommunication;
import com.cinchapi.concourse.server.plugin.io.MessageQueue;

/**
 * A special {@link Plugin} that receives {@link Packet packets} of data for
 * real time {@link #handlePacket(Packet) processing}.
 * 
 * @author Jeff Nelson
 */
@PackagePrivate
abstract class RealTimePlugin extends Plugin {

    /**
     * The name of the fromServer attribute that contains the location of this
     * Plugin's real time stream.
     */
    final static String STREAM_ATTRIBUTE = "stream";

    /**
     * Construct a new instance.
     * 
     * @param station
     * @param notifier
     */
    public RealTimePlugin(String fromServer, String fromPlugin) {
        super(fromServer, fromPlugin);
    }

    /**
     * An {@link Executor} that contains worker threads to handle {@link Packet
     * packets} of data that is streamed to this plugin in real time.
     */
    private final ExecutorService workers = Executors.newCachedThreadPool();

    @Override
    public final void run() {
        Reflection.call(this, "setReadyState");
        // For a RealTimePlugin, the first fromServer message contains the
        // address for the stream channel
        ByteBuffer data = fromServer.read();
        RemoteMessage message = serializer.deserialize(data);
        if(message.type() == RemoteMessage.Type.ATTRIBUTE) {
            RemoteAttributeExchange attribute = (RemoteAttributeExchange) message;
            if(attribute.key().equalsIgnoreCase(STREAM_ATTRIBUTE)) {
                log.debug("Listening for streamed packets at {}",
                        attribute.value());
                @SuppressWarnings("resource") final InterProcessCommunication stream = new MessageQueue(
                        attribute.value());
                // Create a separate event loop to process Packets of writes
                // that come from the server.
                Thread loop = new Thread(() -> {
                    ByteBuffer bytes = null;
                    while ((bytes = stream.read()) != null) {
                        final Packet packet = serializer.deserialize(bytes);

                        // Each packet should be processed in a separate
                        // worker thread
                        workers.execute(() -> {
                            log.debug(
                                    "Received packet from Concourse Server: {}",
                                    packet);
                            handlePacket(packet);
                        });
                    }
                });
                loop.setDaemon(true);
                loop.start();

                // Start normal plugin operations
                super.run();
            }
            else {
                throw new IllegalStateException(
                        "Unsupported attribute " + attribute);
            }
        }
        else {
            throw new IllegalStateException();
        }
    }

    /**
     * Process a {@link Packet} that is streamed from the server.
     * <p>
     * This method is called asynchronously by the parent class, so the subclass
     * doesn't need to fork the implementation to a separate thread.
     * </p>
     * <p>
     * <strong>NOTE:</strong> It isn't possible for the logic implemented in
     * this method, by the subclass, to use the {@link Plugin#runtime
     * ConcourseRuntime} to make upstream calls to Concourse Server. It is
     * possible to work around this limitation by forking a
     * {@link BackgroundThread background thread} using the
     * {@link PluginExecutors} framework; however this is not recommended.
     * Making upstream calls to Concourse Server in this method defeats the
     * purpose of incrementally streaming writes to the plugin. If you feel the
     * need to do this, rethink your implementation.
     * </p>
     * 
     * @param packet a {@link Packet} that contains one or more
     *            {@link Packet.Data} objects.
     */
    protected abstract void handlePacket(Packet packet);

}
