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
package com.cinchapi.concourse.server.model;

import java.nio.ByteBuffer;
import java.util.Comparator;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.cinchapi.concourse.Link;
import com.cinchapi.concourse.server.io.Byteable;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.thrift.Type;
import com.cinchapi.concourse.util.ByteBuffers;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.Numbers;

/**
 * A Value is an abstraction for a {@link TObject} that records type information
 * and serves as the most basic element of data in Concourse. Values are
 * logically sortable using weak typing and cannot exceed 2^32 bytes.
 * <p>
 * <h2>Storage Requirements</h2>
 * Each Value requires at least {@value #CONSTANT_SIZE} bytes of space in
 * addition to the following type specific requirements:
 * <ul>
 * <li>BOOLEAN requires an additional 1 byte</li>
 * <li>DOUBLE requires an additional 8 bytes</li>
 * <li>FLOAT requires an additional 4 bytes</li>
 * <li>INTEGER requires an additional 4 bytes</li>
 * <li>LONG requires an additional 8 bytes</li>
 * <li>LINK requires an additional 8 bytes</li>
 * <li>STRING requires an additional 14 bytes for every character (uses UTF8
 * encoding)</li>
 * </ul>
 * </p>
 * 
 * @author Jeff Nelson
 */
@Immutable
public final class Value implements Byteable, Comparable<Value> {

    /**
     * Return the Value encoded in {@code bytes} so long as those bytes adhere
     * to the format specified by the {@link #getBytes()} method. This method
     * assumes that all the bytes in the {@code bytes} belong to the Value. In
     * general, it is necessary to get the appropriate Value slice from the
     * parent ByteBuffer using {@link ByteBuffers#slice(ByteBuffer, int, int)}.
     * 
     * @param bytes
     * @return the Value
     */
    public static Value fromByteBuffer(ByteBuffer bytes) {
        Type type = Type.values()[bytes.get()];
        TObject data = extractTObjectAndCache(bytes, type);
        return new Value(data, bytes);
    }

    /**
     * Return the optimized Value of {@code value}.
     * 
     * @param value
     * @return the optimized Value
     */
    public static Value optimize(Value value) {
        if(value.getType() == Type.TAG) {
            return Value.wrap(Convert
                    .javaToThrift(value.getObject().toString()));
        }
        return value;
    }

    /**
     * Return a Value that is backed by {@code data}.
     * 
     * @param data
     * @return the Value
     */
    public static Value wrap(TObject data) {
        Object obj = data.getServerWrapper(); /* (Authorized) */
        if(obj == null) {
            // We cache the Value that wraps the TObject, onto the TObject
            // itself to prevent unnecessary creation of additional wrappers
            // throughout the Engine when TObjects and Values are converted
            // back-and-forth (interface-based programming for the win, right).
            // TObject is defined in the client, which doesn't have access to
            // this Value class, so the #serverWrapper attribute of the TObject
            // is a generic object. Thats not ideal, but this approach means
            // that we only pay the penalty for a type cast (which can be JIT
            // optimized) as opposed to the penalty for object creation when
            // wrapping the same TObject to a Value more than once
            Value value = new Value(data);
            data.cacheServerWrapper(value);
            return value;
        }
        else if(obj instanceof Value) {
            return (Value) obj;
        }
        else {
            // We should never get here because this means that someone
            // deliberately cached a garbage value, which shouldn't happen once
            // the TObject is re-constructed by the server.
            data.cacheServerWrapper(null);
            return wrap(data);
        }
    }

    /**
     * Return the {@link TObject} of {@code type} represented by {@code bytes}.
     * This method reads the remaining bytes from the current position into the
     * returned TObject.
     * 
     * @param bytes
     * @param type
     * @return the TObject
     */
    private static TObject extractTObjectAndCache(ByteBuffer bytes, Type type) {
        // Must allocate a heap buffer because TObject assumes it has a
        // backing array and because of THRIFT-2104 that buffer must wrap a
        // byte array in order to assume that the TObject does not lose data
        // when transferred over the wire.
        byte[] array = new byte[bytes.remaining()];
        bytes.get(array); // We CANNOT simply slice {@code buffer} and use
                          // the slice's backing array because the backing
                          // array of the slice is the same as the
                          // original, which contains more data than we
                          // need for the quantity
        return new TObject(ByteBuffer.wrap(array), type);
    }

    /**
     * Check to see if the specific {@code type} is numeric.
     * 
     * @param type
     * @return {@code true} if the type is numeric
     */
    private static boolean isNumericType(Type type) {
        return type == Type.DOUBLE || type == Type.FLOAT
                || type == Type.INTEGER || type == Type.LONG;
    }

    /**
     * A constant representing the smallest possible Value. This should be used
     * in normal operations, but should only be used to indicate an infinite
     * range.
     */
    public static Value NEGATIVE_INFINITY = Value.wrap(Convert
            .javaToThrift(Long.MIN_VALUE));

    /**
     * A constant representing the largest possible Value. This shouldn't be
     * used in normal operations, but should only be used to indicate an
     * infinite range.
     */
    public static Value POSITIVE_INFINITY = Value.wrap(Convert
            .javaToThrift(Long.MAX_VALUE));

    /**
     * The minimum number of bytes needed to encode every Value.
     */
    private static final int CONSTANT_SIZE = 1; // type(1)

    /**
     * A cached copy of the binary representation that is returned from
     * {@link #getBytes()}.
     */
    @Nullable
    private transient ByteBuffer bytes = null;

    /**
     * The underlying data represented by this Value. This representation is
     * used when serializing/deserializing the data for RPC or disk and network
     * I/O.
     */
    private final TObject data;

    /**
     * The java representation of the underlying {@link #data}. This
     * representation is used when interacting with other components in the JVM.
     */
    @Nullable
    private transient Object object = null;

    /**
     * Construct a new instance.
     * 
     * @param data
     */
    private Value(TObject data) {
        this(data, null);
    }

    /**
     * Construct a new instance.
     * 
     * @param data
     * @param bytes
     */
    private Value(TObject data, @Nullable ByteBuffer bytes) {
        this.data = data;
        this.bytes = bytes;
    }

    @Override
    public int compareTo(Value other) {
        return Sorter.INSTANCE.compare(this, other);
    }

    @Override
    public void copyTo(ByteBuffer buffer) {
        buffer.put((byte) data.getType().ordinal());
        buffer.put(data.bufferForData());
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Value) {
            final Value other = (Value) obj;
            Type typeA = getType();
            Type typeB = other.getType();
            if(typeA != typeB && (isNumericType(typeA) && isNumericType(typeB))) {
                return Numbers.isEqualTo((Number) getObject(),
                        (Number) other.getObject());
            }
            else {
                return data.equals(other.data);
            }
        }
        return false;
    }

    /**
     * Return a byte buffer that represents this Value with the following order:
     * <ol>
     * <li><strong>type</strong> - position 0</li>
     * <li><strong>data</strong> - position 1</li>
     * </ol>
     * 
     * @return the ByteBuffer representation
     */
    @Override
    public ByteBuffer getBytes() {
        if(bytes == null) {
            bytes = ByteBuffer.allocate(size());
            copyTo(bytes);
            bytes.rewind();
        }
        return ByteBuffers.asReadOnlyBuffer(bytes);
    }

    /**
     * Return the java object that is represented by this Value.
     * 
     * @return the object representation
     */
    public Object getObject() {
        if(object == null) {
            object = Convert.thriftToJava(data);
        }
        return object;
    }

    /**
     * Return the TObject that is represented by this Value.
     * 
     * @return the TObject representation
     */
    public TObject getTObject() {
        return data;
    }

    /**
     * Return the {@link Type} that describes the underlying data represented by
     * this Value.
     * 
     * @return the type
     */
    public Type getType() {
        return data.getType();
    }

    @Override
    public int hashCode() {
        return data.hashCode();
    }

    /**
     * Return {@code true} if the value {@link #getType() type} is numeric.
     * 
     * @return {@code true} if the value type is numeric
     */
    public boolean isNumericType() {
        return isNumericType(getType());
    }

    @Override
    public int size() {
        return CONSTANT_SIZE + data.data.capacity();
    }

    @Override
    public String toString() {
        return getObject().toString() + " (" + getType() + ")";
    }

    /**
     * A {@link Comparator} that is used to sort Values using weak typing.
     * 
     * @author Jeff Nelson
     */
    public static enum Sorter implements Comparator<Value> {
        INSTANCE;

        @Override
        public int compare(Value v1, Value v2) {
            if((v1 == POSITIVE_INFINITY && v2 == POSITIVE_INFINITY)
                    || (v1 == NEGATIVE_INFINITY && v2 == NEGATIVE_INFINITY)) {
                return 0;
            }
            else if(v1 == POSITIVE_INFINITY) {
                return 1;
            }
            else if(v2 == POSITIVE_INFINITY) {
                return -1;
            }
            else if(v1 == NEGATIVE_INFINITY) {
                return -1;
            }
            else if(v2 == NEGATIVE_INFINITY) {
                return 1;
            }
            else {
                Object o1 = v1.getObject();
                Object o2 = v2.getObject();
                if(o1 instanceof Number
                        && o2 instanceof Number
                        && ((!(o1 instanceof Link) && !(o2 instanceof Link)) || (o1 instanceof Link && o2 instanceof Link))) {
                    return Numbers.compare((Number) o1, (Number) o2);
                }
                else if(o1 instanceof Number) {
                    return -1;
                }
                else if(o2 instanceof Number) {
                    return 1;
                }
                else {
                    return o1.toString().compareToIgnoreCase(o2.toString());
                }
            }

        }
    }

}
