/*
 * This project is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 * 
 * This project is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this project. If not, see <http://www.gnu.org/licenses/>.
 */
package com.cinchapi.common.io;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

import com.google.common.primitives.Longs;

/**
 * Thread safe {@link ByteBuffer} utilities.
 * 
 * @author jnelson
 */
public final class ByteBuffers {

	private static Charset charset = StandardCharsets.UTF_8;

	/**
	 * Return the charset used for encoding.
	 * 
	 * @return the charset.
	 */
	public static Charset charset() {
		return charset;
	}

	/**
	 * Return the value represented by {@code buffer} while maintaining the
	 * buffer's current position.
	 * 
	 * @param buffer
	 * @return the value.
	 */
	public static Boolean getBoolean(ByteBuffer buffer) {
		int position = buffer.position();
		Boolean value = buffer.get() > 0 ? true : false;
		buffer.position(position);
		return value;
	}

	/**
	 * Return the value represented by {@code buffer} while maintaining the
	 * buffer's current position.
	 * 
	 * @param buffer
	 * @return the value.
	 */
	public static Double getDouble(ByteBuffer buffer) {
		int position = buffer.position();
		Double value = buffer.getDouble();
		buffer.position(position);
		return value;
	}

	/**
	 * Return the value represented by {@code buffer} while maintaining the
	 * buffer's current position.
	 * 
	 * @param buffer
	 * @return the value.
	 */
	public static Float getFloat(ByteBuffer buffer) {
		int position = buffer.position();
		Float value = buffer.getFloat();
		buffer.position(position);
		return value;
	}

	/**
	 * Return the value represented by {@code buffer} while maintaining the
	 * buffer's current position.
	 * 
	 * @param buffer
	 * @return the value.
	 */
	public static Integer getInt(ByteBuffer buffer) {
		int position = buffer.position();
		Integer value = buffer.getInt();
		buffer.position(position);
		return value;
	}

	/**
	 * Return the value represented by {@code buffer} while maintaining the
	 * buffer's current position.
	 * 
	 * @param buffer
	 * @return the value.
	 */
	public static Long getLong(ByteBuffer buffer) {
		int position = buffer.position();
		Long value = buffer.getLong();
		buffer.position(position);
		return value;
	}

	/**
	 * Return the value represented by {@code buffer} while maintaining the
	 * buffer's current position.
	 * 
	 * @param buffer
	 * @return the value.
	 */
	public static String getString(ByteBuffer buffer) {
		String value = "";
		int position = buffer.position();
		try {
			CharsetDecoder decoder = charset.newDecoder();
			decoder.onMalformedInput(CodingErrorAction.IGNORE);
			value = decoder.decode(buffer).toString().trim(); // it
																// is
																// necessary
																// to
																// trim
																// here
																// because
																// the
																// decoding
																// picks
																// up
																// trailing
																// whitespace
																// sometimes
		}
		catch (CharacterCodingException e) {
			e.printStackTrace();
		}
		buffer.position(position);
		return value;
	}

	/**
	 * Return a byte buffer that contains {@code value}.
	 * 
	 * @param value
	 * @return the byte buffer.
	 */
	public static ByteBuffer toByteBuffer(Boolean value) {
		Byte _value = value ? (byte) 1 : (byte) 0;
		ByteBuffer buffer = ByteBuffer.wrap(ByteBuffer.allocate(1).put(_value)
				.array());
		return buffer;
	}

	/**
	 * Return a byte buffer that contains {@code value}.
	 * 
	 * @param value
	 * @return the byte buffer.
	 */
	public static ByteBuffer toByteBuffer(Double value) {
		ByteBuffer buffer = ByteBuffer.wrap(ByteBuffer.allocate(8)
				.putDouble(value).array());
		return buffer;
	}

	/**
	 * Return a byte buffer that contains {@code value}.
	 * 
	 * @param value
	 * @return the byte buffer.
	 */
	public static ByteBuffer toByteBuffer(Float value) {
		ByteBuffer buffer = ByteBuffer.wrap(ByteBuffer.allocate(4)
				.putFloat(value).array());
		return buffer;
	}

	/**
	 * Return a byte buffer that contains {@code value}.
	 * 
	 * @param value
	 * @return the byte buffer.
	 */
	public static ByteBuffer toByteBuffer(Integer value) {
		ByteBuffer buffer = ByteBuffer.wrap(ByteBuffer.allocate(4)
				.putInt(value).array());
		return buffer;
	}

	/**
	 * Return a byte buffer that contains {@code value}.
	 * 
	 * @param value
	 * @return the byte buffer.
	 */
	public static ByteBuffer toByteBuffer(Long value) {
		ByteBuffer buffer = ByteBuffer.wrap(Longs.toByteArray(value));
		return buffer;
	}

	/**
	 * Return a byte buffer that contains {@code value}.
	 * 
	 * @param value
	 * @return the byte buffer.
	 */
	public static ByteBuffer toByteBuffer(String value) {
		ByteBuffer buffer = null;
		try {
			CharsetEncoder encoder = charset().newEncoder();
			encoder.onMalformedInput(CodingErrorAction.IGNORE);
			buffer = encoder.encode(CharBuffer.wrap(value));
		}
		catch (CharacterCodingException e) {
			e.printStackTrace();
		}
		buffer.limit(buffer.capacity()); // for some reason the buffer's limit
											// is lower than its capacity so
											// i'm setting it here so it is
											// consistent with buffers that are
											// read from a file/channel
		return buffer;
	}

	private ByteBuffers() {}

}
