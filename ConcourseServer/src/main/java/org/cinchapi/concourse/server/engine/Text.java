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
package org.cinchapi.concourse.server.engine;

import java.nio.ByteBuffer;

import javax.annotation.concurrent.Immutable;

import org.cinchapi.common.annotate.DoNotInvoke;
import org.cinchapi.common.annotate.PackagePrivate;
import org.cinchapi.common.io.ByteBufferOutputStream;
import org.cinchapi.common.io.ByteBuffers;
import org.cinchapi.common.io.Byteable;
import org.cinchapi.common.io.Byteables;

/**
 * A {@link Storable} UTF-8 sequence of characters.
 * 
 * @author jnelson
 */
@Immutable
@PackagePrivate
final class Text implements Byteable, Comparable<Text> {

	/**
	 * Return the Text encoded in {@code buffer} so long as those bytes adhere
	 * to the format specified by the {@link #getBytes()} method. This method
	 * assumes that all the bytes in the {@code buffer} belong to the Text. In
	 * general, it is necessary to get the appropriate Text slice from the
	 * parent ByteBuffer using {@link ByteBuffers#slice(ByteBuffer, int, int)}.
	 * 
	 * @param buffer
	 * @return the Text
	 */
	public static Text fromByteBuffer(ByteBuffer buffer) {
		return Byteables.read(buffer, Text.class); // We are using
													// Byteables#read(ByteBuffer,
													// Class) instead of calling
													// the constructor directly
													// so as to take advantage
													// of the automatic
													// reference caching that is
													// provided in the utility
													// class
	}

	/**
	 * Return Text that is not appropriate for storage, but can be used in
	 * comparisons. This is the preferred way to create Texts unless the Text
	 * will be stored.
	 * 
	 * @param string
	 * @return the Text
	 */
	public static Text fromString(String string) {
		return new Text(string);
	}

	/**
	 * Encode {@code text} and {@code timestamp} into a ByteBuffer that
	 * conforms to the format specified for {@link Text#getBytes()}.
	 * 
	 * @param quantity
	 * @param timestamp
	 * @return the ByteBuffer encoding
	 */
	static ByteBuffer encodeAsByteBuffer(Object text) {
		ByteBufferOutputStream out = new ByteBufferOutputStream();
		out.write(text);
		ByteBuffer bytes = out.toByteBuffer();
		out.close();
		return bytes;
	}

	/**
	 * The maximum number of bytes that can be used to encode a single Text.
	 */
	@PackagePrivate
	static final int MAX_SIZE = Integer.MAX_VALUE;

	/**
	 * Represents an empty text string, which has a timestamp of
	 * {@value Storable#NIL} and occupies {@value #CONSTANT_SIZE} bytes.
	 */
	@PackagePrivate
	static final Text EMPTY = Text.fromString("");

	/**
	 * <p>
	 * In order to optimize heap usage, we encode the Text as a single
	 * ByteBuffer instead of storing each component as a member variable.
	 * </p>
	 * <p>
	 * To retrieve a component, we navigate to the appropriate position and
	 * convert the necessary bytes to the correct type, which is a cheap since
	 * binary conversion is trivial. Once a component is loaded onto the heap,
	 * it may be stored in an ReferenceCache for further future efficiency.
	 * </p>
	 * 
	 * The content conforms to the specification described by the
	 * {@link #getBytes()} method.
	 */
	private final ByteBuffer bytes;

	/**
	 * Construct an instance that represents an existing Text from a
	 * ByteBuffer. This constructor is public so as to comply with the
	 * {@link Byteable} interface. Calling this constructor directly is not
	 * recommend. Use {@link #fromByteBuffer(ByteBuffer)} instead to take
	 * advantage of reference caching.
	 * 
	 * @param bytes
	 */
	@DoNotInvoke
	public Text(ByteBuffer bytes) {
		this.bytes = bytes;
	}

	/**
	 * Construct a notForStorage instance.
	 * 
	 * @param text
	 */
	private Text(String text) {
		this.bytes = encodeAsByteBuffer(text);
	}

	@Override
	public int compareTo(Text o) {
		return toString().compareTo(o.toString());
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof Text) {
			Text other = (Text) obj;
			return toString().equals(other.toString());
		}
		return false;
	}

	@Override
	public synchronized ByteBuffer getBytes() {
		ByteBuffer clone = ByteBuffers.clone(bytes);
		clone.rewind();
		return clone;
	}

	@Override
	public int hashCode() {
		return toString().hashCode();
	}

	@Override
	public int size() {
		return bytes.capacity();
	}

	@Override
	public String toString() {
		return ByteBuffers.getString(getBytes());
	}

}
