/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2014 Jeff Nelson, Cinchapi Software Collective
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.cinchapi.concourse.server.io;

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

import org.cinchapi.concourse.util.ByteBuffers;

import com.google.common.base.Throwables;

/**
 * A collection of methods to make native java serialization/deserialization
 * more convenient and interoperable with NIO utilities like file channels and
 * byte buffers.
 * 
 * @author jnelson
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