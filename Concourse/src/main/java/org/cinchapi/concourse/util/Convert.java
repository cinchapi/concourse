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
package org.cinchapi.concourse.util;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.cinchapi.common.annotate.UtilityClass;
import org.cinchapi.common.io.ByteBufferOutputStream;
import org.cinchapi.common.io.ByteBuffers;
import org.cinchapi.concourse.Link;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.thrift.Type;
import org.joda.time.DateTime;

/**
 * A collection of functions to convert objects. The public API defined in
 * {@link Concourse} uses certain objects for convenience that are not
 * recognized by Thrift, so it is necessary to convert back and forth between
 * different representations.
 * 
 * @author jnelson
 */
@UtilityClass
public final class Convert {

	/**
	 * Return the Thrift Object that represents {@code object}.
	 * 
	 * @param object
	 * @return the TObject
	 */
	public static TObject javaToThrift(Object object) {
		ByteBufferOutputStream out = new ByteBufferOutputStream();
		Type type = null;
		if(object instanceof Boolean) {
			out.write((boolean) object);
			type = Type.BOOLEAN;
		}
		else if(object instanceof Double) {
			out.write((double) object);
			type = Type.DOUBLE;
		}
		else if(object instanceof Float) {
			out.write((float) object);
			type = Type.FLOAT;
		}
		else if(object instanceof Link) {
			out.write(((Link) object).longValue());
			type = Type.LINK;
		}
		else if(object instanceof Long) {
			out.write((long) object);
			type = Type.LONG;
		}
		else if(object instanceof Integer) {
			out.write((int) object);
			type = Type.INTEGER;
		}
		else {
			out.write(object.toString());
			type = Type.STRING;
		}
		out.close();
		return new TObject(out.toByteBuffer(), type);
	}

	/**
	 * Return a long that represents the same Unix timestamp with microsecond
	 * precision as {@link timestamp}.
	 * 
	 * @param timestamp
	 * @return the Unix timestamp
	 */
	public static long jodaToUnix(DateTime timestamp) {
		return TimeUnit.MICROSECONDS.convert(timestamp.getMillis(),
				TimeUnit.MILLISECONDS);
	}

	/**
	 * Return the Java Object that represents {@code object}.
	 * 
	 * @param object
	 * @return the Object
	 */
	public static Object thriftToJava(TObject object) {
		Object java = null;
		ByteBuffer buffer = object.bufferForData();
		buffer.position(0);
		switch (object.getType()) {
		case BOOLEAN:
			java = ByteBuffers.getBoolean(buffer);
			break;
		case DOUBLE:
			java = buffer.getDouble();
			break;
		case FLOAT:
			java = buffer.getFloat();
			break;
		case INTEGER:
			java = buffer.getInt();
			break;
		case LINK:
			java = Link.to(buffer.getLong());
			break;
		case LONG:
			java = buffer.getLong();
			break;
		default:
			java = ByteBuffers.getString(buffer);
			break;
		}
		buffer.rewind();
		return java;
	}

	/**
	 * Return a {@link DateTime} object that represents the same Unix timestamp
	 * with microsecond precision as {@code timestamp}.
	 * 
	 * @param timestamp
	 * @return the DateTime object
	 */
	public static DateTime unixToJoda(long timestamp) {
		return new DateTime(TimeUnit.MILLISECONDS.convert(timestamp,
				TimeUnit.MICROSECONDS));
	}

	private Convert() {/* Utility Class */}

}
