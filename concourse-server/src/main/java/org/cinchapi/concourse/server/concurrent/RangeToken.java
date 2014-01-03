/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2014 Jeff Nelson, Cinchapi Software Collective
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
package org.cinchapi.concourse.server.concurrent;

import java.nio.ByteBuffer;
import java.util.Arrays;

import javax.annotation.Nullable;

import org.cinchapi.concourse.server.model.Text;
import org.cinchapi.concourse.server.model.Value;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.util.ByteBuffers;

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
 * @author jnelson
 */
public class RangeToken extends Token {

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
            values = Arrays.copyOf(values, length + 2);
            values[length] = Value.POSITIVE_INFINITY;
            values[length + 1] = Value.NEGATIVE_INFINITY;

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
        bytes.put(key.getBytes());
        for (Value value : values) {
            bytes.putInt(value.size());
            bytes.put(value.getBytes());
        }
        return bytes;
    }

    /**
     * A flag to indicate that {@link #operator} is NULL in the serialized form.
     */
    private static final byte NULL_OPERATOR = (byte) Operator.values().length;

    private final Text key;

    @Nullable
    private final Operator operator;
    private final Value[] values;

    /**
     * Construct a new instance.
     * 
     * @param key
     * @param operator
     * @param values
     */
    private RangeToken(Text key, Operator operator, Value... values) {
        super(serialize(key, operator, values));
        this.key = key;
        this.operator = operator;
        this.values = values;
    }

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
}
