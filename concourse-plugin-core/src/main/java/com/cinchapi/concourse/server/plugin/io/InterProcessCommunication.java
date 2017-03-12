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

import java.nio.ByteBuffer;

import javax.annotation.concurrent.ThreadSafe;

/**
 * {@link InterProcessCommunication} is a means by which two or more processes
 * can communicate with one another by passing messages.
 * 
 * @author Jeff Nelson
 */
@ThreadSafe
public interface InterProcessCommunication {

    /**
     * If supported, run compaction on the underlying communication
     * infrastructure to optimize how much space is utilized.
     */
    public void compact();

    /**
     * Read the most recent message, blocking until a message is available.
     * 
     * @return a {@link ByteBuffer that contains the most recent message
     */
    public ByteBuffer read();

    /**
     * Write a message that can be read by all other participating
     * {@link #read() reader processes}.
     * <p>
     * <strong>CAUTION:</strong> This method does not check to make sure that
     * the most recent message was read before writing.
     * </p>
     * 
     * @param message the message to write to the memory segment
     * @return {@link InterProcessCommunication this} for chaining
     */
    public InterProcessCommunication write(ByteBuffer message);

}
