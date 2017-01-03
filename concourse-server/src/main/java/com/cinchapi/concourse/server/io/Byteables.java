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
package com.cinchapi.concourse.server.io;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.Map;

import com.cinchapi.concourse.util.ByteBuffers;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;

/**
 * Contains static factory methods to construct {@link Byteable} objects from
 * ByteBuffers.
 * 
 * @author Jeff Nelson
 */
public abstract class Byteables {

    /**
     * Return an instance of {@code classObj} by reading {@code bytes}. This
     * method uses reflection to invoke the single argument ByteBuffer
     * constructor in {@code classObj}.
     * <p>
     * <tt>Byteables.read(bytes, Foo.class)</tt>
     * </p>
     * It is assumed that all the contents of {@code bytes} are relevant to the
     * object being read, so so call
     * {@link ByteBuffers#slice(ByteBuffer, int, int)} or follow this
     * protocol when using this method:
     * <ul>
     * <li>Set the position of the parent ByteBuffer to the index of the first
     * byte relevant to the object, using {@link ByteBuffer#position(int)}.</li>
     * <li>Set the limit of the parent ByteBuffer to the index of its current
     * position + the size of the object (which is usually stored in the 4 bytes
     * preceding the object) using {@link ByteBuffer#limit(int)}.</li>
     * <li>Slice the parent buffer using {@link ByteBuffer#slice()}, which will
     * create a child buffer with the same content of the parent buffer between
     * its current position and its limit.</li>
     * </ul>
     * 
     * @param bytes
     * @param classObj
     * @return an instance of {@code classObj} read from {@code bytes}
     */
    @SuppressWarnings("unchecked")
    public static <T> T read(ByteBuffer bytes, Class<T> classObj) {
        try {
            Constructor<T> constructor = (Constructor<T>) constructorCache
                    .get(classObj);
            if(constructor == null) {
                constructor = classObj.getDeclaredConstructor(ByteBuffer.class);
                constructor.setAccessible(true);
                constructorCache.put(classObj, constructor);
            }
            return constructor.newInstance(bytes);
        }
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Return an instance of {@code className} by reading {@code bytes}. This
     * method uses reflection to invoke the single argument ByteBuffer
     * constructor in {@code className}.
     * <p>
     * <tt>Byteables.{@literal <Foo>}read(bytes, com.organization.module.Foo)</tt>
     * </p>
     * It is assumed that all the contents of {@code bytes} are relevant to the
     * object being read, so call
     * {@link ByteBuffers#slice(ByteBuffer, int, int)} or follow this
     * protocol when using this method:
     * <ul>
     * <li>Set the position of the parent ByteBuffer to the index of the first
     * byte relevant to the object, using {@link ByteBuffer#position(int)}.</li>
     * <li>Set the limit of the parent ByteBuffer to the index of its current
     * position + the size of the object (which is usually stored in the 4 bytes
     * preceding the object) using {@link ByteBuffer#limit(int)}.</li>
     * <li>Slice the parent buffer using {@link ByteBuffer#slice()}, which will
     * create a child buffer with the same content of the parent buffer between
     * its current position and its limit.</li>
     * </ul>
     * 
     * @param bytes
     * @param className the fully qualified class name
     * @return an instance of {@code className} read from {@code bytes}
     */
    @SuppressWarnings("unchecked")
    public static <T> T read(ByteBuffer bytes, final String className) {
        try {
            return (T) read(bytes, Class.forName(className));
        }
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }

    }

    /**
     * Return an instance of {@code classObj} by reading {@code bytes}. This
     * method uses reflection to invoke the single argument static method named
     * <strong>fromByteBuffer</strong> in {@code classObj}.
     * <p>
     * <tt>Byteables.read(bytes, Foo.class)</tt>
     * </p>
     * It is assumed that all the contents of {@code bytes} are relevant to the
     * object being read, so so call
     * {@link ByteBuffers#slice(ByteBuffer, int, int)} or follow this
     * protocol when using this method:
     * <ul>
     * <li>Set the position of the parent ByteBuffer to the index of the first
     * byte relevant to the object, using {@link ByteBuffer#position(int)}.</li>
     * <li>Set the limit of the parent ByteBuffer to the index of its current
     * position + the size of the object (which is usually stored in the 4 bytes
     * preceding the object) using {@link ByteBuffer#limit(int)}.</li>
     * <li>Slice the parent buffer using {@link ByteBuffer#slice()}, which will
     * create a child buffer with the same content of the parent buffer between
     * its current position and its limit.</li>
     * </ul>
     * 
     * @param bytes
     * @param classObj
     * @return an instance of {@code classObj} read from {@code bytes}
     */
    @SuppressWarnings("unchecked")
    public static <T> T readStatic(ByteBuffer bytes, Class<T> classObj) {
        try {
            Method method = staticFactoryCache.get(classObj);
            if(method == null) {
                method = classObj.getMethod("fromByteBuffer", ByteBuffer.class);
                staticFactoryCache.put(classObj, method);
            }
            return (T) method.invoke(null, bytes);
        }
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Return an instance of {@code className} by reading {@code bytes}. This
     * method uses reflection to invoke the single argument static method named
     * <strong>fromByteBuffer</strong> in {@code className}.
     * <p>
     * <tt>Byteables.read(bytes, Foo.class)</tt>
     * </p>
     * It is assumed that all the contents of {@code bytes} are relevant to the
     * object being read, so so call
     * {@link ByteBuffers#slice(ByteBuffer, int, int)} or follow this
     * protocol when using this method:
     * <ul>
     * <li>Set the position of the parent ByteBuffer to the index of the first
     * byte relevant to the object, using {@link ByteBuffer#position(int)}.</li>
     * <li>Set the limit of the parent ByteBuffer to the index of its current
     * position + the size of the object (which is usually stored in the 4 bytes
     * preceding the object) using {@link ByteBuffer#limit(int)}.</li>
     * <li>Slice the parent buffer using {@link ByteBuffer#slice()}, which will
     * create a child buffer with the same content of the parent buffer between
     * its current position and its limit.</li>
     * </ul>
     * 
     * @param bytes
     * @param classObj
     * @return an instance of {@code className} read from {@code bytes}
     */
    @SuppressWarnings("unchecked")
    public static <T> T readStatic(ByteBuffer bytes, String className) {
        try {
            return (T) readStatic(bytes, Class.forName(className));
        }
        catch (ClassNotFoundException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Write {@code object} to {@code channel} starting at the channel's current
     * position.
     * 
     * @param obj
     * @param channel
     */
    public static void write(Byteable object, FileChannel channel) {
        try {
            FileLock lock = channel.lock(channel.position(), object.size(),
                    false);
            channel.write(object.getBytes());
            channel.force(true);
            lock.release();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }

    }

    /**
     * Cache of constructors that are captured using reflection.
     */
    private static final Map<Class<?>, Constructor<?>> constructorCache = Maps
            .newIdentityHashMap();

    /**
     * Cache of static factory methods that are captured using reflection.
     */
    private static final Map<Class<?>, Method> staticFactoryCache = Maps
            .newIdentityHashMap();

}
