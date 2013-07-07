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
import org.cinchapi.common.cache.ReferenceCache;
import org.cinchapi.common.io.ByteBufferOutputStream;
import org.cinchapi.common.io.ByteBuffers;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.thrift.Type;

/**
 * A collection of utility methods that are used in the {@link Value} class.
 * 
 * @author jnelson
 */
@PackagePrivate
abstract class Values {

	/**
	 * Encode {@code quantity} and {@code timestamp} into a ByteBuffer that
	 * conforms to the format specified for {@link Value#getBytes()}.
	 * 
	 * @param quantity
	 * @param timestamp
	 * @return the ByteBuffer encoding
	 */
	public static ByteBuffer encodeAsByteBuffer(TObject quantity, long timestamp) {
		ByteBufferOutputStream out = new ByteBufferOutputStream();
		out.write(timestamp);
		out.write(quantity.getType().ordinal());
		out.write(quantity.bufferForData());
		ByteBuffer bytes = out.toByteBuffer();
		out.close();
		return bytes;
	}

	/**
	 * Get an object of {@code type} from {@code buffer}. This method will read
	 * starting from the current position up until enough bytes for {@code type}
	 * have been read. If {@code type} equals {@link Type#STRING}, all of the
	 * remaining bytes in the buffer will be read.
	 * 
	 * @param buffer
	 * @param type
	 * @return the object.
	 */
	static TObject getQuantityFromByteBuffer(ByteBuffer buffer, Type type) {
		Object[] cacheKey = { ByteBuffers.encodeAsHexString(buffer), type };
		TObject object = quantityCache.get(cacheKey);
		if(object == null) {
			// Must allocate a heap buffer because TObject assumes it has a
			// backing array.
			object = new TObject(ByteBuffer.allocate(buffer.remaining()).put(
					buffer), type);
			quantityCache.put(object, cacheKey);
		}
		return object;
	}

	/**
	 * Maintains a cache of all the quantities that are extracted from
	 * ByteBuffers in the {@link #getQuantityFromByteBuffer(ByteBuffer, Type)}
	 * method.
	 */
	private static final ReferenceCache<TObject> quantityCache = new ReferenceCache<TObject>();

}
