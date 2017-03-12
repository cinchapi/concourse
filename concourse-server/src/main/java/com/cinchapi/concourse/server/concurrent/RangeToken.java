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
package com.cinchapi.concourse.server.concurrent;

import java.nio.ByteBuffer;
import java.util.Arrays;

import javax.annotation.Nullable;

import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.util.ByteBuffers;

/**
 * A specialized {@link Token} that is used to define the scope of a lock
 * acquired by a {@link RangeLockService}.
 * <p>
 * In general, callers interested in range locking should not use a RangeToken
 * directly but should feed the relevant data to
 * {@link RangeLockService#getReadLock(Text, Operator, Value...)} or
 * {@link RangeLockService#getWriteLock(Text, Value)}.
 * </p>
 * 
 * @author Jeff Nelson
 */
public class RangeToken extends Token {

    /**
     * Return a {@link RangeToken} that wraps {@code key} {@code operator}
     * {@code values} for the purpose of reading.
     * 
     * @param key
     * @param operator
     * @param values
     * @return the RangeToken
     */
    public static RangeToken forReading(Text key, Operator operator,
            Value... values) {
        // Check to see what, if any, additional range values must be added to
        // properly block writers that may interfere with our read.
        int length = values.length;
        if(operator == Operator.GREATER_THAN
                || operator == Operator.GREATER_THAN_OR_EQUALS) {
            values = Arrays.copyOf(values, length + 1);
            values[length] = Value.POSITIVE_INFINITY;
        }
        else if(operator == Operator.LESS_THAN
                || operator == Operator.LESS_THAN_OR_EQUALS) {
            values = Arrays.copyOf(values, length + 1);
            values[length] = Value.NEGATIVE_INFINITY;
        }
        else if(operator == Operator.REGEX || operator == Operator.NOT_REGEX) {
            // NOTE: This will block any writers on the #key whenever there is a
            // REGEX or NOT_REGEX read, which isn't the most efficient approach,
            // but is the least burdensome, which is okay for now...
            values = ALL_VALUES;
        }
        return new RangeToken(key, operator, values);
    }

    /**
     * Return a {@link RangeToken} that wraps {@code key} as {@code value} for
     * the purpose of writing.
     * 
     * @param key
     * @param value
     * @return the RangeToken
     */
    public static RangeToken forWriting(Text key, Value value) {
        return new RangeToken(key, null, value);
    }

    /**
     * Return the RangeToken encoded in {@code bytes} so long as those bytes
     * adhere to the format specified by the {@link #getBytes()} method. This
     * method assumes that all the bytes in the {@code bytes} belong to the
     * RangeToken. In general, it is necessary to get the appropriate RangeToken
     * slice from the parent ByteBuffer using
     * {@link ByteBuffers#slice(ByteBuffer, int, int)}.
     * 
     * @param bytes
     * @return the RangeToken
     */
    public static RangeToken fromByteBuffer(ByteBuffer bytes) {
        return new RangeToken(bytes);
    }

    /**
     * Return the ByteBuffer with the serialized form appropriate for a
     * RangeToken that describes {@code key} {@code operator} {@code values}.
     * 
     * @param key
     * @param operator
     * @param values
     * @return the ByteBuffer
     */
    private static ByteBuffer serialize(Text key, Operator operator,
            Value... values) {
        int size = 1 + 4 + key.size() + (4 * values.length);
        for (Value value : values) {
            size += value.size();
        }
        ByteBuffer bytes = ByteBuffer.allocate(size);
        bytes.put(operator != null ? (byte) operator.ordinal() : NULL_OPERATOR);
        bytes.putInt(key.size());
        key.copyTo(bytes);
        for (Value value : values) {
            bytes.putInt(value.size());
            value.copyTo(bytes);
        }
        bytes.rewind();
        return bytes;
    }

    /**
     * A static reference to the range that indicates a RangeToken covers the
     * entire range of values.
     */
    private static final Value[] ALL_VALUES = { Value.NEGATIVE_INFINITY,
            Value.POSITIVE_INFINITY };

    /**
     * A flag to indicate that {@link #operator} is NULL in the serialized form.
     */
    private static final byte NULL_OPERATOR = (byte) Operator.values().length;

    /**
     * An empty byte buffer that is passed off to the super constructor.
     */
    private static final ByteBuffer NULL_BYTES = ByteBuffer.wrap(new byte[0]);

    private final Text key;

    @Nullable
    private final Operator operator;
    private final Value[] values;

    @Nullable
    private ByteBuffer bytes;

    /**
     * Construct a new instance.
     * 
     * @param bytes
     */
    private RangeToken(ByteBuffer bytes) {
        super(bytes);
        byte operator = bytes.get();
        this.operator = operator == NULL_OPERATOR ? null
                : Operator.values()[operator];
        this.key = Text.fromByteBuffer(ByteBuffers.get(bytes, bytes.getInt()));
        this.values = new Value[this.operator == Operator.BETWEEN ? 2 : 1];
        int i = 0;
        while (bytes.hasRemaining()) {
            values[i] = Value.fromByteBuffer(ByteBuffers.get(bytes,
                    bytes.getInt()));
        }
    }

    /**
     * Construct a new instance.
     * 
     * @param key
     * @param operator
     * @param values
     */
    private RangeToken(Text key, Operator operator, Value... values) {
        super(NULL_BYTES);
        this.key = key;
        this.operator = operator;
        this.values = values;
    }

    /**
     * Return the {@code key} associated with this RangeToken.
     * 
     * @return the key
     */
    public Text getKey() {
        return key;
    }

    /**
     * Return the {@code operator} associated with this RangeToken, if it
     * exists.
     * 
     * @return the operator
     */
    @Nullable
    public Operator getOperator() {
        return operator;
    }

    /**
     * Return the collection of {@code values} associated with this RangeToken.
     * 
     * @return the values
     */
    public Value[] getValues() {
        return values;
    }

    /**
     * Return {@code true} if this RangeToken intersects the {@code other}
     * RangeToken. This RangeToken is considered to intersect another one if the
     * left point of this RangeToken is less than or equal to the right point of
     * the other one and the right point of this RangeToken is greater than or
     * equal to the left point of the other one.
     * 
     * @param other
     * @return {@code true} if this RangeToken intersects the other
     */
    public boolean intersects(RangeToken other) {
        Value value = other.values[0];
        Value myValue = this.values[0];
        Operator myOperator = this.operator == null ? Operator.EQUALS
                : this.operator;
        Operator operator = other.operator == null ? Operator.EQUALS
                : other.operator;
        switch (myOperator) {
        case EQUALS:
            switch (operator) {
            case EQUALS:
                return myValue.compareTo(value) == 0;
            case NOT_EQUALS:
                return myValue.compareTo(value) != 0;
            case GREATER_THAN:
                return myValue.compareTo(value) > 0;
            case GREATER_THAN_OR_EQUALS:
                return myValue.compareTo(value) >= 0;
            case LESS_THAN:
                return myValue.compareTo(value) < 0;
            case LESS_THAN_OR_EQUALS:
                return myValue.compareTo(value) <= 0;
            case BETWEEN:
                return myValue.compareTo(value) >= 0
                        && myValue.compareTo(other.values[1]) < 0;
            case REGEX:
            case NOT_REGEX:
                return true;
            default:
                throw new UnsupportedOperationException();
            }
        case NOT_EQUALS:
            switch (operator) {
            case EQUALS:
                return myValue.compareTo(value) != 0;
            case NOT_EQUALS:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUALS:
            case LESS_THAN:
            case LESS_THAN_OR_EQUALS:
                return true;
            case BETWEEN:
                return myValue.compareTo(value) != 0
                        || myValue.compareTo(other.values[1]) != 0;
            case REGEX:
            case NOT_REGEX:
                return true;
            default:
                throw new UnsupportedOperationException();
            }
        case GREATER_THAN:
            switch (operator) {
            case EQUALS:
            case LESS_THAN:
            case LESS_THAN_OR_EQUALS:
                return value.compareTo(myValue) > 0;
            case NOT_EQUALS:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUALS:
                return true;
            case BETWEEN:
                return other.values[1].compareTo(myValue) > 0;
            case REGEX:
            case NOT_REGEX:
                return true;
            default:
                throw new UnsupportedOperationException();
            }
        case GREATER_THAN_OR_EQUALS:
            switch (operator) {
            case EQUALS:
            case LESS_THAN_OR_EQUALS:
                return value.compareTo(myValue) >= 0;
            case LESS_THAN:
                return value.compareTo(myValue) > 0;
            case NOT_EQUALS:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUALS:
                return true;
            case BETWEEN:
                return other.values[1].compareTo(myValue) > 0; // end of range
                                                               // not
                // included for BETWEEN
            case REGEX:
            case NOT_REGEX:
                return true;
            default:
                throw new UnsupportedOperationException();
            }
        case LESS_THAN:
            switch (operator) {
            case EQUALS:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUALS:
                return value.compareTo(myValue) < 0;
            case NOT_EQUALS:
            case LESS_THAN:
            case LESS_THAN_OR_EQUALS:
                return true;
            case BETWEEN:
                return value.compareTo(myValue) < 0;
            case REGEX:
            case NOT_REGEX:
                return true;
            default:
                throw new UnsupportedOperationException();
            }
        case LESS_THAN_OR_EQUALS:
            switch (operator) {
            case EQUALS:
            case GREATER_THAN_OR_EQUALS:
                return value.compareTo(myValue) <= 0;
            case LESS_THAN:
            case LESS_THAN_OR_EQUALS:
            case NOT_EQUALS:
                return true;
            case GREATER_THAN:
                return value.compareTo(myValue) < 0;
            case BETWEEN:
                return value.compareTo(myValue) <= 0;
            case REGEX:
            case NOT_REGEX:
                return true;
            default:
                throw new UnsupportedOperationException();
            }
        case BETWEEN:
            Value myOtherValue = this.values[1];
            switch (operator) {
            case EQUALS:
                return value.compareTo(myValue) >= 0
                        && value.compareTo(myOtherValue) < 0;
            case NOT_EQUALS:
                return value.compareTo(myValue) != 0
                        || value.compareTo(myOtherValue) != 0;
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUALS:
                return value.compareTo(myOtherValue) < 0;
            case LESS_THAN:
                return value.compareTo(myValue) > 0;
            case LESS_THAN_OR_EQUALS:
                return value.compareTo(myValue) >= 0;
            case BETWEEN:
                return myOtherValue.compareTo(value) >= 0
                        && myValue.compareTo(other.values[1]) <= 0;
            case REGEX:
            case NOT_REGEX:
                return true;
            default:
                throw new UnsupportedOperationException();
            }
        case REGEX:
        case NOT_REGEX:
            return true;
        default:
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(key);
        sb.append(operator != null ? " " + operator : " AS");
        for (Value value : values) {
            sb.append(" " + value);
        }
        return sb.toString();
    }

    @Override
    public ByteBuffer getBytes() {
        if(bytes == null) {
            bytes = serialize(key, operator, values);
        }
        return ByteBuffers.asReadOnlyBuffer(bytes);
    }

    @Override
    public int size() {
        if(bytes == null) {
            bytes = serialize(key, operator, values);
        }
        return bytes.capacity();
    }

    @Override
    public void copyTo(ByteBuffer buffer) {
        if(bytes == null) {
            bytes = serialize(key, operator, values);
        }
        ByteBuffers.copyAndRewindSource(bytes, buffer);
    }

}
