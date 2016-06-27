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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.cinchapi.concourse.annotate.PackagePrivate;
import com.cinchapi.concourse.server.plugin.io.SharedMemory;
import com.cinchapi.concourse.util.ByteBuffers;
import com.cinchapi.concourse.util.Serializables;

/**
 * A special {@link Plugin} that receives {@link Packet packets} of data for
 * real time {@link #handlePacket(Packet) processing}.
 * 
 * @author Jeff Nelson
 */
@PackagePrivate
abstract class RealTimePlugin extends Plugin {

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
        // In the case of a RealTimePlugin, the first fromServer message
        // contains the address for the stream channel
        ByteBuffer data = fromServer.read();
        Instruction type = ByteBuffers.getEnum(data, Instruction.class);
        data = ByteBuffers.getRemaining(data);
        if(type == Instruction.MESSAGE) {
            String stream0 = ByteBuffers.getString(data);
            final SharedMemory stream = new SharedMemory(stream0);

            // Create a separate event loop to process Packets of writes that
            // come from the server.
            Thread loop = new Thread(new Runnable() {

                @Override
                public void run() {
                    ByteBuffer data = null;
                    while ((data = stream.read()) != null) {
                        final Packet packet = Serializables.read(data,
                                Packet.class);

                        // Each packet should be processed in a separate worker
                        // thread
                        workers.execute(new Runnable() {

                            @Override
                            public void run() {
                                handlePacket(packet);
                            }

                        });
                    }

                }

            });
            loop.setDaemon(true);
            loop.start();

            // Start normal plugin operations
            super.run();
        }
        else {
            throw new IllegalStateException();
        }
    }

    /**
     * Process a {@link Packet} that is streamed from the server.
     * 
     * @param packet a {@link Packet} that contains one or more
     *            {@link Packet.Data} objects.
     */
    protected abstract void handlePacket(Packet packet);

}
