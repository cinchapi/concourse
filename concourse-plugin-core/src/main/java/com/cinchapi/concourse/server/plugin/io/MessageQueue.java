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
package com.cinchapi.concourse.server.plugin.io;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.concourse.server.plugin.concurrent.FileLocks;
import com.cinchapi.concourse.util.FileOps;
import com.cinchapi.concourse.util.Strings;
import com.google.common.collect.Maps;

/**
 * A form of {@link InterProcessCommunication} that uses anonymous sockets.
 * <p>
 * A {@link MessageQueue} allows a processes to communicate with one or more
 * other processes using anonymous sockets. Messages sent from any processes are
 * guaranteed to be sent to all other processes that have connected to the
 * {@link MessageQueue} at the time to the message is written. While all
 * connected processes receive messages, they will only see them by calling the
 * {@link #read()} method. The {@link #read()} method blocks until messages are
 * available. All reads are guaranteed to happen sequentially, so each process
 * will read all the messages that have been sent since the process connected to
 * the {@link MessageQueue} in the order that they were sent.
 * </p>
 * 
 * @author Jeff Nelson
 */
public class MessageQueue implements InterProcessCommunication, AutoCloseable {

    /**
     * The host to use when constructing socket connections.
     */
    private static final String SOCKET_HOST = "localhost";

    /**
     * The {@link FileChannel} that contains metadata for the queue.
     */
    private final FileChannel metadata;

    /**
     * The local process port on which the queue listens for messages.
     */
    private final int port;

    /**
     * The local process channel on which the queue receives messages.
     */
    private final ServerSocketChannel channel;

    /**
     * The {@link Thread} that is responsible for accepting and processing
     * messages.
     */
    private final Thread acceptor;

    /**
     * A {@link Queue} containing all the unread messages that have been
     * received since this instance as created.
     */
    private final BlockingQueue<ByteBuffer> messages;

    /**
     * A collection of {@link SocketChannel} for all the readers that should be
     * notified about {@link #write(ByteBuffer) written} messages.
     */
    private final Map<Integer, SocketChannel> readers = Maps.newHashMap();

    /**
     * Construct a new instance.
     */
    public MessageQueue() {
        this(FileOps.tempFile("con", ".mq"));
    }

    /**
     * Construct a new instance.
     * 
     * @param file
     */
    public MessageQueue(String file) {
        try {
            this.metadata = FileChannel.open(Paths.get(file).toAbsolutePath(),
                    StandardOpenOption.CREATE, StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
            this.messages = new LinkedBlockingQueue<>();

            // Setup the ServerSocketChannel to receive messages from writers
            this.channel = ServerSocketChannel.open();
            Selector selector = Selector.open();
            InetSocketAddress address = new InetSocketAddress(SOCKET_HOST, 0);
            channel.bind(address);
            this.port = ((InetSocketAddress) channel.getLocalAddress())
                    .getPort();
            channel.configureBlocking(false);
            int ops = channel.validOps();
            channel.register(selector, ops);

            // Inform all the writers about the port on which we're listening
            register();

            // Start a thread that is dedicating to accepting writers and
            // placing their messages onto the #queue
            this.acceptor = new Thread(() -> {
                while (true) {
                    try {
                        selector.select();
                        Iterator<SelectionKey> keys = selector.selectedKeys()
                                .iterator();
                        while (keys.hasNext()) {
                            SelectionKey key = keys.next();
                            if(key.isAcceptable()) {
                                SocketChannel writer = channel.accept();
                                writer.configureBlocking(false);
                                writer.register(selector, SelectionKey.OP_READ);
                            }
                            if(key.isReadable()) {
                                SocketChannel writer = (SocketChannel) key
                                        .channel();
                                ByteBuffer size = ByteBuffer.allocate(4);
                                writer.read(size);
                                size.flip();
                                if(size.limit() > 0) { // if limit < 0, no
                                                       // message was written
                                    ByteBuffer message = ByteBuffer
                                            .allocate(size.getInt());
                                    while (message.hasRemaining()) {
                                        writer.read(message);
                                    }
                                    message.flip();
                                    messages.add(message);
                                }
                            }
                            keys.remove();
                        }
                    }
                    catch (ClosedByInterruptException e) {/* no-op */}
                    catch (IOException e) {
                        throw CheckedExceptions.throwAsRuntimeException(e);
                    }

                }
            });
            acceptor.setDaemon(true);
            acceptor.setUncaughtExceptionHandler((t, e) -> {
                RuntimeException ex = new RuntimeException(Strings.format(
                        "Uncaught exception in Thread {}: {}", t, e), e);
                ex.printStackTrace();
            });
            acceptor.start();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    close();
                }
                catch (Exception e) {
                    throw CheckedExceptions.throwAsRuntimeException(e);
                }
            }));
        }
        catch (IOException e) {
            throw CheckedExceptions.throwAsRuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        acceptor.interrupt();
        channel.close();
        for (Entry<Integer, SocketChannel> entry : readers.entrySet()) {
            SocketChannel reader = entry.getValue();
            reader.close();
        }
    }

    @Override
    public void compact() {/* no-op */}

    @Override
    public ByteBuffer read() {
        try {
            return messages.take();
        }
        catch (InterruptedException e) {
            throw CheckedExceptions.throwAsRuntimeException(e);
        }
    }

    @Override
    public InterProcessCommunication write(ByteBuffer message) {
        FileLock lock = lock();
        try {
            ByteBuffer bytes = ByteBuffer.allocate((int) metadata.size());
            metadata.position(0);
            metadata.read(bytes);
            bytes.flip();
            while (bytes.hasRemaining()) {
                int port = bytes.getInt();
                SocketChannel reader = readers.get(port);
                if(reader == null) {
                    InetSocketAddress address = new InetSocketAddress(
                            SOCKET_HOST, port);
                    reader = SocketChannel.open(address);
                    readers.put(port, reader);
                }
                ByteBuffer size = ByteBuffer.allocate(4)
                        .putInt(message.capacity());
                size.flip();
                reader.write(size);
                while (message.hasRemaining()) {
                    reader.write(message);
                }
                message.flip();
            }
            return this;
        }
        catch (IOException e) {
            throw CheckedExceptions.throwAsRuntimeException(e);
        }
        finally {
            FileLocks.release(lock);
        }
    }

    /**
     * Lock the underlying {@link #metadata} to prevent additional readers from
     * being added, and to prevent concurrent writes from happening.
     * 
     * @return the {@link FileLock} for the {@link #metadata}
     */
    private FileLock lock() {
        return FileLocks.lock(metadata, 0L, Long.MAX_VALUE, false);
    }

    /**
     * Register this instance by writing the port on which it listens for
     * messages in the underlying {@link #metadata}.
     */
    private void register() {
        FileLock lock = lock();
        try {
            // Write the socket's listener port to the metadata so that it is
            // visible to writers
            ByteBuffer data = ByteBuffer.allocate(4);
            data.putInt(port);
            data.flip();
            metadata.write(data, metadata.size());
            metadata.force(true);
        }
        catch (IOException e) {
            throw CheckedExceptions.throwAsRuntimeException(e);
        }
        finally {
            FileLocks.release(lock);
        }
    }

    @Override
    public String toString() {
        return Strings.format("MessageQueue[pending = {}]", messages);
    }

}
