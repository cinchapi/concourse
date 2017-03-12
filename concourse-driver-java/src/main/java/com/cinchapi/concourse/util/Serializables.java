/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

import com.google.common.base.Throwables;

/**
 * A collection of methods to make native java serialization/deserialization
 * more convenient and interoperable with NIO utilities like file channels and
 * byte buffers.
 * 
 * @author Jeff Nelson
 */
public final class Serializables {

    /**
     * Return the binary representation for the {@link Serializable}
     * {@code object} in the form of a {@link ByteBuffer}.
     * 
     * @param object
     * @return the binary representation
     */
    public static ByteBuffer getBytes(Serializable object) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutput output = new ObjectOutputStream(
                    new BufferedOutputStream(baos));
            output.writeObject(object);
            output.flush();
            output.close();
            return ByteBuffer.wrap(baos.toByteArray());
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Read back the serialized object of class {@code classObj} from the
     * specified sequence of {@code bytes}.
     * 
     * @param bytes
     * @param classObj
     * @return the object
     */
    @SuppressWarnings("unchecked")
    public static <T extends Serializable> T read(ByteBuffer bytes,
            Class<T> classObj) {
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(
                    ByteBuffers.toByteArray(bytes));
            BufferedInputStream bis = new BufferedInputStream(bais);
            ObjectInput input = new ObjectInputStream(bis);
            T object = (T) input.readObject();
            return object;
        }
        catch (IOException | ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Read back the serialized object of {@code className} from the specified
     * sequence of {@code bytes}.
     * 
     * @param bytes
     * @param className
     * @return
     */
    @SuppressWarnings("unchecked")
    public static <T extends Serializable> T read(ByteBuffer bytes,
            final String className) {
        try {
            Class<T> clazz = (Class<T>) Class.forName(className);
            return read(bytes, clazz);
        }
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Write {@code object} to {@code channel} starting at the channel's current
     * position.
     * 
     * @param object
     * @param channel
     */
    public static void write(Serializable object, FileChannel channel) {
        try {
            ByteBuffer bytes = getBytes(object);
            FileLock lock = channel.lock(channel.position(), bytes.capacity(),
                    false);
            channel.write(bytes);
            channel.force(true);
            lock.release();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }

    }

    private Serializables() {/* noop */}

}
