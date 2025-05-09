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
package com.cinchapi.concourse.server.model;

import java.nio.ByteBuffer;
import java.util.Comparator;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.cinchapi.common.io.ByteBuffers;
import com.cinchapi.concourse.server.io.ByteSink;
import com.cinchapi.concourse.server.io.Byteable;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.thrift.Type;
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
     * A constant representing the smallest possible Value. This should be used
     * in normal operations, but should only be used to indicate an infinite
     * range.
     */
    public static Value NEGATIVE_INFINITY = Value
            .wrap(TObject.NEGATIVE_INFINITY);

    /**
     * A constant representing the largest possible Value. This shouldn't be
     * used in normal operations, but should only be used to indicate an
     * infinite range.
     */
    public static Value POSITIVE_INFINITY = Value
            .wrap(TObject.POSITIVE_INFINITY);

    /**
     * The largest integer/long that can be represented by a Double without
     * losing precision. This value is derived from the fact that the mantissa
     * is 53 bytes.
     */
    protected static long MAX_DOUBLE_REPRESENTED_INTEGER = (long) Math.pow(2,
            53);

    /**
     * The smallest integer/long that can be represented by a Double without
     * losing precision. This value is derived from the fact that the mantissa
     * is 53 bytes.
     */
    protected static long MIN_DOUBLE_REPRESENTED_INTEGER = -1
            * MAX_DOUBLE_REPRESENTED_INTEGER;

    /**
     * The minimum number of bytes needed to encode every Value.
     */
    private static final int CONSTANT_SIZE = 1; // type(1)

    /**
     * Return the Value encoded in {@code bytes} so long as those bytes adhere
     * to the format specified by the {@link #getBytes()} method.
     * 
     * @param bytes
     * @return the Value
     */
    public static Value fromByteBuffer(ByteBuffer bytes) {
        Type type = Type.values()[bytes.get()];
        TObject data = createTObject(bytes, type);
        return new Value(data);
    }

    /**
     * Return the optimized Value of {@code value}.
     * 
     * @param value
     * @return the optimized Value
     */
    public static Value optimize(Value value) {
        if(value.getType() == Type.TAG) {
            return Value
                    .wrap(Convert.javaToThrift(value.getObject().toString()));
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
    private static TObject createTObject(ByteBuffer bytes, Type type) {
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
        return type == Type.DOUBLE || type == Type.FLOAT || type == Type.INTEGER
                || type == Type.LONG;
    }

    /**
     * A cached copy of the binary representation that is returned from
     * {@link #getCanonicalBytes()}.
     */
    @Nullable
    private ByteBuffer cbytes = null;

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
     * @param bytes
     */
    private Value(TObject data) {
        this.data = data;
    }

    @Override
    public int compareTo(Value other) {
        return Sorter.INSTANCE.compare(this, other);
    }

    /**
     * Compare this and the {@code other} {@link Value} while ignoring any
     * differences in case.
     * 
     * @param other
     * @return the case insensitive comparison value
     */
    public int compareToIgnoreCase(Value other) {
        return getTObject().compareToIgnoreCase(other.getTObject());
    }

    @Override
    public void copyCanonicalBytesTo(ByteBuffer buffer) {
        copyCanonicalBytesTo(ByteSink.to(buffer));
    }

    @Override
    public void copyCanonicalBytesTo(ByteSink sink) {
        if(cbytes != null) {
            sink.put(getCanonicalBytes());
        }
        else {
            if(isNumericType()) {
                // Must canonicalize numbers so that integer and floating point
                // representations have the same binary form if those
                // representations are essentially equal (i.e. 18 vs 18.0). We
                // do this by storing every number as a double unless its an
                // integer that can't be stored as a double without losing
                // precision, in which case we store it as a long.
                Number number = (Number) getObject();
                if(number instanceof Long && (number
                        .longValue() < MIN_DOUBLE_REPRESENTED_INTEGER
                        || number
                                .longValue() > MAX_DOUBLE_REPRESENTED_INTEGER)) {
                    sink.putLong(number.longValue());
                }
                else {
                    // Must parse the Double from a string (instead of calling
                    // number#doubleValue()) because a Float that looks like a
                    // double is actually represented with less precision and
                    // will suffer from widening primitive conversion.
                    sink.putDouble(Double.parseDouble(number.toString()));
                }
            }
            else if(isCharSequenceType()) {
                sink.putUtf8(getObject().toString());
            }
            else {
                Byteable.super.copyCanonicalBytesTo(sink);
            }
        }
    }

    @Override
    public void copyTo(ByteSink sink) {
        sink.put((byte) data.getType().ordinal()); // type
        sink.put(data.bufferForData()); // data
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Value) {
            final Value other = (Value) obj;
            Type typeA = getType();
            Type typeB = other.getType();
            if(typeA != typeB
                    && (isNumericType(typeA) && isNumericType(typeB))) {
                return Numbers.areEqual((Number) getObject(),
                        (Number) other.getObject());
            }
            else {
                return data.equals(other.data);
            }
        }
        return false;
    }

    /**
     * Compares this {@link Value} to another one while ignoring case
     * considerations.
     * 
     * @param obj
     * @return a boolean that indicates whether {@code obj} is equal to this
     *         {@link Value}, regardless of case
     */
    public boolean equalsIgnoreCase(Value obj) {
        if(obj.isCharSequenceType() && isCharSequenceType()) {
            return ((Value) obj).toLowerCase().equals(toLowerCase());
        }
        else {
            return equals(obj);
        }
    }

    @Override
    public ByteBuffer getCanonicalBytes() {
        if(cbytes == null) {
            ByteBuffer cbytes = ByteBuffer.allocate(getCanonicalLength());
            copyCanonicalBytesTo(ByteSink.to(cbytes));
            cbytes.flip();
            this.cbytes = cbytes;
        }
        return ByteBuffers.asReadOnlyBuffer(cbytes);
    }

    @Override
    public int getCanonicalLength() {
        if(isNumericType()) {
            return 8;
        }
        else if(isCharSequenceType()) {
            return size() - CONSTANT_SIZE;
        }
        else {
            return Byteable.super.getCanonicalLength();
        }
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
     * Return {@code true} if the value {@link #getType() type} is a character
     * sequence.
     * 
     * @return {@code true} if the value type is a character sequence
     */
    public boolean isCharSequenceType() {
        return getTObject().isCharSequenceType();
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
     * Convert this {@link Value} to its uppercase form.
     * <p>
     * If this {@link Value} isn't a {@link #isCharSequenceType() character
     * sequence} or can't be uppercased, this {@link Value} is returned.
     * </p>
     * 
     * @return the uppercased {@link Value}
     */
    public Value toUpperCase() {
        if(isCharSequenceType()) {
            return wrap(
                    Convert.javaToThrift(getObject().toString().toUpperCase()));
        }
        else {
            return this;
        }
    }

    /**
     * Convert this {@link Value} to its lowercase form.
     * <p>
     * If this {@link Value} isn't a {@link #isCharSequenceType() character
     * sequence} or can't be lowercased, this {@link Value} is returned.
     * </p>
     * 
     * @return the lowercased {@link Value}
     */
    public Value toLowerCase() {
        if(isCharSequenceType()) {
            return wrap(
                    Convert.javaToThrift(getObject().toString().toLowerCase()));
        }
        else {
            return this;
        }
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
            return TObject.comparator().compare(v1.getTObject(),
                    v2.getTObject());
        }
    }

}
