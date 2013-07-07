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

import org.cinchapi.common.annotate.PackagePrivate;
import org.cinchapi.common.annotate.UtilityClass;
import org.cinchapi.common.io.ByteBufferOutputStream;

/**
 * A collection of utility methods that are used in the {@link Write} class.
 * 
 * @author jnelson
 */
@PackagePrivate
@UtilityClass
final class Writes {

	/**
	 * Return a string that describes the revision encapsulated in the Write.
	 * 
	 * @param write
	 * @return a description of the Write
	 */
	public static String describe(Write write) {
		String verb = write.getType().name();
		String key = write.getKey().toString();
		String value = write.getValue().toString();
		String preposition = write.getType() == WriteType.ADD ? "TO" : "FROM";
		String record = write.getRecord().toString();
		return new StringBuilder().append(verb).append(" ").append(key)
				.append(" ").append("AS").append(" ").append(value).append(" ")
				.append(preposition).append(" ").append(record).append(" ")
				.toString();
	}

	/**
	 * Encode the Write of {@code type} {@code key} as {@code value} in
	 * {@code record} into a ByteBuffer that conforms to the format specified in
	 * {@link Write#getBytes()}.
	 * 
	 * @param type
	 * @param key
	 * @param value
	 * @param record
	 * @return the ByteBuffer encoding
	 */
	public static ByteBuffer encodeAsByteBuffer(WriteType type, Text key,
			Value value, PrimaryKey record) {
		ByteBufferOutputStream out = new ByteBufferOutputStream();
		out.write(type);
		out.write(record);
		out.write(key.size());
		out.write(value.size());
		out.write(key);
		out.write(value);
		out.close();
		return out.toByteBuffer();
	}

	/**
	 * Return the keySize that is encoded in {@code bytes}.
	 * 
	 * @param bytes
	 * @return the keySize
	 */
	public static int getKeySize(ByteBuffer bytes) {
		bytes.position(Write.KEY_SIZE_POS);
		return bytes.getInt();
	}

	/**
	 * Return the valueSize that is encoded in {@code bytes}.
	 * 
	 * @param bytes
	 * @return the valueSize
	 */
	public static int getValueSize(ByteBuffer bytes) {
		bytes.position(Write.VALUE_SIZE_POS);
		return bytes.getInt();
	}

	private Writes() {/* non-initializable */}

}
