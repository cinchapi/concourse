/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013 Jeff Nelson, Cinchapi Software Collective
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

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;

import com.google.common.collect.Lists;

/**
 * This class implements a <em>concurrent</em> output stream in which data is
 * written into a byte buffer. The stream automatically grows as data as written
 * to it. The data can be retrieved using the {@link #toByteBuffer()},
 * {@link #toDirectByteBuffer()} , and {@link #toMappedByteBuffer(String, long)}
 * methods.
 * <p>
 * Closing a {@code ByteBufferOutputStream} has no effect. The methods in this
 * class can be called after the stream has been closed without generating an
 * {@code IOException}.
 * </p>
 * 
 * @author jnelson
 */
public class ByteBufferOutputStream extends OutputStream {

    /**
     * We use an ArrayList to model a dynamically growing byte buffer output
     * stream.
     */
    private final ArrayList<Byte> stream;

    /**
     * Construct a new byte buffer output stream.
     */
    public ByteBufferOutputStream() {
        this(32);
    }

    /**
     * Construct a new byte buffer output stream with an initial capacity of the
     * specified {@code size} in bytes.
     * 
     * @param size
     */
    public ByteBufferOutputStream(int size) {
        this.stream = Lists.newArrayListWithCapacity(size);
    }

    /**
     * Closing a <tt>ByteBufferOutputStream</tt> has no effect. The methods in
     * this class can be called after the stream has been closed without
     * generating an <tt>IOException</tt>.
     */
    @Override
    public void close() {}

    /**
     * Return the current size of the stream
     * 
     * @return the number of valid bytes in the stream
     */
    public int size() {
        return stream.size();
    }

    /**
     * Create a newly allocated ByteBuffer that contains the content of the
     * stream. The ByteBuffer will have a capacity and limit of {@link #size()}
     * and a position of 0.
     * 
     * @return the current content of the output stream as a ByteBuffer
     */
    public ByteBuffer toByteBuffer() {
        return toByteBuffer(ByteBuffer.allocate(size()));
    }

    /**
     * Create a newly allocated DirectByteBuffer that contains the content of
     * the
     * stream. The ByteBuffer will have a capacity and limit of {@link #size()}
     * and a position of 0.
     * 
     * @return the current content of the output stream as a ByteBuffer
     */
    public ByteBuffer toDirectByteBuffer() {
        return toByteBuffer(ByteBuffer.allocateDirect(size()));
    }

    /**
     * Create a newly allocated MappedByteBuffer that contains the content of
     * the stream. The ByteBuffer will have a capacity and limit of
     * {@link #size()} and a position of 0.
     * 
     * @return the current content of the output stream as a ByteBuffer
     */
    public MappedByteBuffer toMappedByteBuffer(String file, long position) {
        return (MappedByteBuffer) toByteBuffer(FileSystem.map(file,
                MapMode.READ_WRITE, position, size()));
    }

    /**
     * Write a byte that represents {@code value} to the stream.
     * 
     * @param value
     */
    public void write(boolean value) {
        write(value ? (byte) 1 : (byte) 0);
    }

    /**
     * Write {@code value} to the stream.
     * 
     * @param value
     */
    public void write(byte value) {
        stream.add(value);
    }

    @Override
    public void write(byte[] bytes) {
        for (byte b : bytes) {
            write(b);
        }
    }

    @Override
    public void write(byte[] b, int off, int len) {
        throw new UnsupportedOperationException();
    }

    /**
     * Write the bytes for {@code value} to the stream.
     * 
     * @param value
     */
    public void write(Byteable value) {
        write(value.getBytes());
    }

    /**
     * Write the remaining bytes from {@code buffer} to the stream.
     * 
     * @param buffer
     */
    public void write(ByteBuffer buffer) {
        while (buffer.hasRemaining()) {
            write(buffer.get());
        }

    }

    /**
     * Write the bytes for {@code value} to the stream.
     * 
     * @param value
     */
    public void write(double value) {
        write(Double.doubleToRawLongBits(value));
    }

    /**
     * Write the bytes for the enum ordinal of {@code value} to the stream.
     * 
     * @param value
     */
    public void write(Enum<?> value) {
        write(value.ordinal());
    }

    /**
     * Write the bytes for {@code value} to the stream.
     * 
     * @param value
     */
    public void write(float value) {
        write(Float.floatToRawIntBits(value));
    }

    /**
     * Write the bytes for {@code value} to the stream.
     * 
     * @param value
     */
    @Override
    public void write(int value) {
        write((byte) (value >> 24));
        write((byte) (value >> 16));
        write((byte) (value >> 8));
        write((byte) (value));
    }

    /**
     * Write the bytes for {@code value} to the stream.
     * 
     * @param value
     */
    public void write(long value) {
        write((byte) (value >> 56));
        write((byte) (value >> 48));
        write((byte) (value >> 40));
        write((byte) (value >> 32));
        write((byte) (value >> 24));
        write((byte) (value >> 16));
        write((byte) (value >> 8));
        write((byte) (value));
    }

    /**
     * Write the bytes for {@code value} to the stream. This method will check
     * to see if {@code value} is an instance of a primitive type and write the
     * bytes accordingly. If {@code value} is not a primitive type, then a UTF-8
     * encoded byte array from the toString() method will be written to the
     * stream.
     * 
     * @param value
     */
    public void write(Object value) {
        if(value instanceof Byte) {
            write((byte) value);
        }
        else if(value instanceof Integer) {
            write((int) value);
        }
        else if(value instanceof Boolean) {
            write((boolean) value);
        }
        else if(value instanceof Long) {
            write((long) value);
        }
        else if(value instanceof Float) {
            write((float) value);
        }
        else if(value instanceof Double) {
            write((double) value);
        }
        else {
            write(value.toString());
        }
    }

    /**
     * UTF-8 encode {@code value} and write the bytes to the stream.
     * 
     * @param value
     */
    public void write(String value) {
        write(value, StandardCharsets.UTF_8);
    }

    /**
     * Encode {@code value} using {@code charset} and write the bytes to the
     * stream.
     * 
     * @param value
     * @param charset
     */
    public void write(String value, Charset charset) {
        write(value.getBytes(charset));
    }

    /**
     * Write the bytes for {@code value} to the stream.
     * 
     * @param value
     */
    public void write(Collection<? extends Byteable> value) {
        write(ByteableCollections.toByteBuffer(value));
    }

    /**
     * Write the bytes for {@code value} to the stream.
     * 
     * @param value
     * @param sizePerElement
     */
    public void write(Collection<? extends Byteable> value, int sizePerElement) {
        write(ByteableCollections.toByteBuffer(value, sizePerElement));
    }

    /**
     * Write the contents of the stream to {@code buffer}.
     * 
     * @param buffer
     * @return {@code buffer}
     */
    private ByteBuffer toByteBuffer(ByteBuffer buffer) {
        for (byte b : stream) {
            buffer.put(b);
        }
        buffer.rewind();
        return buffer;
    }

}
