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
package org.cinchapi.concourse.server.model;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import javax.annotation.concurrent.Immutable;

import org.cinchapi.concourse.annotate.DoNotInvoke;
import org.cinchapi.concourse.server.io.Byteable;
import org.cinchapi.concourse.server.io.Byteables;
import org.cinchapi.concourse.util.ByteBuffers;

/**
 * A {@link Byteable} wrapper for a string of UTF-8 encoded characters.
 * 
 * @author jnelson
 */
@Immutable
public final class Text implements Byteable, Comparable<Text> {

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
	 * Return Text that is backed by {@code string}.
	 * 
	 * @param string
	 * @return the Text
	 */
	public static Text fromString(String string) {
		return new Text(string);
	}

	/**
	 * Represents an empty text string.
	 */
	public static final Text EMPTY = Text.fromString("");

	/**
	 * The wrapped string.
	 */
	private final String text;

	/**
	 * Master byte sequence that represents this object. Read-only duplicates
	 * are made when returning from {@link #getBytes()}.
	 */
	private final transient ByteBuffer bytes;

	/**
	 * Construct an instance that represents existing Text from a
	 * ByteBuffer. This constructor is public so as to comply with the
	 * {@link Byteable} interface. Calling this constructor directly is not
	 * recommend. Use {@link #fromByteBuffer(ByteBuffer)} instead to take
	 * advantage of reference caching.
	 * 
	 * @param bytes
	 */
	@DoNotInvoke
	public Text(ByteBuffer bytes) {
		this.text = ByteBuffers.getString(bytes, StandardCharsets.UTF_8);
		this.bytes = bytes;
	}

	/**
	 * Construct an instance that wraps the {@code text} string.
	 * 
	 * @param text
	 */
	private Text(String text) {
		this.text = text;
		this.bytes = ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8));
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
	public ByteBuffer getBytes() {
		return ByteBuffers.asReadOnlyBuffer(bytes);
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
		return text;
	}

}
