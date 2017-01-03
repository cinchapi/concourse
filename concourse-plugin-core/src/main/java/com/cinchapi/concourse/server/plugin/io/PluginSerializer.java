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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.cinchapi.common.io.ByteBuffers;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.server.plugin.RemoteMessage;
import com.cinchapi.concourse.thrift.ComplexTObject;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.thrift.Type;
import com.cinchapi.concourse.util.Serializables;

import io.atomix.catalyst.buffer.Buffer;
import io.atomix.catalyst.buffer.HeapBuffer;

/**
 * A tool for transparently serializing data passed among plugins and Concourse
 * Server, regardless of their type.
 * <p>
 * A {@link PluginSerializer} is a one-stop shop for dealing with all the
 * serialization protocols used by different classes within the framework. It
 * compactly embeds just enough information to be able to represent almost any
 * class.
 * </p>
 * 
 * @author Jeff Nelson
 */
public class PluginSerializer {

    /**
     * Return the object represented by the serialized data in the
     * {@link ByteBuffer}.
     * 
     * @param bytes the bytes from which the object is deserialized
     * @return the deserialized object
     */
    @SuppressWarnings("unchecked")
    public <T> T deserialize(ByteBuffer bytes) {
        Scheme scheme = Scheme.values()[bytes.get()];
        if(scheme == Scheme.PLUGIN_SERIALIZABLE) {
            Buffer buffer = HeapBuffer.wrap(ByteBuffers.toByteArray(bytes));
            buffer.position(bytes.position());
            Class<T> clazz = Reflection.getClassCasted(buffer.readUTF8());
            T instance = Reflection.newInstance(clazz);
            ((PluginSerializable) instance).deserialize(buffer);
            return instance;
        }
        else if(scheme == Scheme.REMOTE_MESSAGE) {
            Buffer buffer = HeapBuffer.wrap(ByteBuffers.toByteArray(bytes));
            buffer.position(bytes.position());
            T instance = (T) RemoteMessage.fromBuffer(buffer);
            return instance;
        }
        else if(scheme == Scheme.COMPLEX_TOBJECT) {
            ByteBuffer bytes0 = ByteBuffers.slice(bytes, 1, bytes.remaining());
            return (T) ComplexTObject.fromByteBuffer(bytes0);
        }
        else if(scheme == Scheme.TOBJECT) {
            ByteBuffer bytes0 = ByteBuffers.slice(bytes, 1, bytes.remaining());
            Type type = Type.values()[bytes0.get()];
            return (T) new TObject(
                    ByteBuffers.slice(bytes0, bytes0.remaining()), type);
        }
        else if(scheme == Scheme.JAVA_SERIALIZABLE) {
            int classLength = bytes.getShort();
            byte[] className = new byte[classLength];
            bytes.get(className);
            Class<T> clazz = Reflection.getClassCasted(new String(className,
                    StandardCharsets.UTF_8));
            bytes = ByteBuffers.get(bytes, bytes.remaining());
            Serializable instance = Serializables.read(bytes,
                    (Class<? extends Serializable>) clazz);
            return (T) instance;
        }
        else {
            // NOTE: In the future, if/when we add support for storing binary
            // blobs, we will need to add a scheme to distinguish a legitimate
            // blob from a blob that is really just a PluginSerializable object
            throw new IllegalStateException(
                    "Cannot plugin deserialize the provided byte stream");
        }
    }

    /**
     * Return a {@link ByteBuffer} that contains the serialized form of the
     * input {@code object}.
     * 
     * @param object the object to serialize
     * @return the serialized form within a ByteBuffer
     */
    public ByteBuffer serialize(Object object) {
        ByteBuffer buffer = null;
        if(object instanceof PluginSerializable) {
            HeapBuffer buffer0 = HeapBuffer.allocate();
            buffer0.writeByte(Scheme.PLUGIN_SERIALIZABLE.ordinal());
            buffer0.writeUTF8(object.getClass().getName());
            ((PluginSerializable) object).serialize(buffer0);
            byte[] bytes = new byte[(int) buffer0.position()];
            buffer0.flip();
            buffer0.read(bytes);
            buffer = ByteBuffer.wrap(bytes);
            return buffer;
        }
        else if(object instanceof RemoteMessage) {
            HeapBuffer buffer0 = (HeapBuffer) ((RemoteMessage) object)
                    .serialize();
            buffer = ByteBuffer.allocate((int) buffer0.remaining() + 1);
            buffer.put((byte) Scheme.REMOTE_MESSAGE.ordinal());
            buffer.put(buffer0.array(), 0, (int) buffer0.remaining());
            buffer.flip();
            return buffer;
        }
        else if(object instanceof ComplexTObject) {
            byte[] bytes = ((ComplexTObject) object).toByteBuffer().array();
            buffer = ByteBuffer.allocate(bytes.length + 1);
            buffer.put((byte) Scheme.COMPLEX_TOBJECT.ordinal());
            buffer.put(bytes);
            buffer.flip();
            return buffer;
        }
        else if(object instanceof TObject) {
            byte[] bytes = ((TObject) object).getData();
            buffer = ByteBuffer.allocate(bytes.length + 2);
            buffer.put((byte) Scheme.TOBJECT.ordinal());
            buffer.put((byte) ((TObject) object).getType().ordinal());
            buffer.put(bytes);
            buffer.flip();
            return buffer;
        }
        else if(object instanceof String || object.getClass().isPrimitive()
                || object instanceof Number || object instanceof Boolean
                || object instanceof Map || object instanceof List
                || object instanceof Set) {
            return serialize(ComplexTObject.fromJavaObject(object));
        }
        else if(object instanceof Serializable) {
            byte[] bytes = ByteBuffers.toByteArray(Serializables
                    .getBytes((Serializable) object));
            byte[] classBytes = object.getClass().getName()
                    .getBytes(StandardCharsets.UTF_8);
            buffer = ByteBuffer.allocate(1 + 2 + classBytes.length
                    + bytes.length);
            buffer.put((byte) Scheme.JAVA_SERIALIZABLE.ordinal());
            buffer.putShort((short) classBytes.length);
            buffer.put(classBytes);
            buffer.put(bytes);
            buffer.flip();
            return buffer;
        }
        else {
            throw new IllegalStateException(
                    "Cannot plugin serialize an object of type "
                            + object.getClass());
        }
    }

    /**
     * A list of all the serialization schemes.
     * 
     * @author Jeff Nelson
     */
    public enum Scheme {
        COMPLEX_TOBJECT,
        JAVA_SERIALIZABLE,
        PLUGIN_SERIALIZABLE,
        TOBJECT,
        REMOTE_MESSAGE;
    }
}
