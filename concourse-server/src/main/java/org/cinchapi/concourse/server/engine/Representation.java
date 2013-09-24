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
import java.util.Arrays;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.cinchapi.concourse.annotate.DoNotInvoke;
import org.cinchapi.concourse.annotate.PackagePrivate;
import org.cinchapi.concourse.cache.ReferenceCache;
import org.cinchapi.concourse.server.concurrent.Lock;
import org.cinchapi.concourse.server.concurrent.Lockable;
import org.cinchapi.concourse.server.concurrent.Lockables;
import org.cinchapi.concourse.server.io.Byteable;
import org.cinchapi.concourse.util.ByteBuffers;

import com.google.common.base.Preconditions;

/**
 * A {@code Representation} is a lightweight delegate for the purpose of
 * providing {@link Lockable} functionality. This is useful in cases when the
 * resource that needs to be locked isn't represented by a single Object, but is
 * still a distinct concept that is possibly made up of multiple Objects.
 * <p>
 * <em><strong>Note:</strong> It is not possible to reconstruct Objects back 
 * from a Representation.</em>
 * </p>
 * 
 * @author jnelson
 */
@PackagePrivate
class Representation implements Lockable, Byteable {

	/**
	 * Return a {@code Representation} encoded in {@code buffer}.
	 * 
	 * @param buffer
	 * @return the Representation
	 */
	public static Representation fromByteBuffer(ByteBuffer buffer) {
		buffer.rewind();
		Representation representation = cache.get(buffer);
		if(representation == null) {
			representation = new Representation(buffer);
			cache.put(representation, buffer);
		}
		return representation;
	}

	/**
	 * Return a {@code Representation} for the {@code objects}.
	 * 
	 * @param objects
	 * @return the Representation
	 */
	public static Representation forObjects(Object... objects) {
		ByteBuffer buffer = ByteBuffer.wrap(DigestUtils.md5(Arrays
				.toString(objects)));
		buffer.rewind();
		Representation representation = cache.get(buffer);
		if(representation == null) {
			representation = new Representation(buffer);
			cache.put(representation, buffer);
		}
		return representation;
	}

	private static final ReferenceCache<Representation> cache = new ReferenceCache<Representation>();

	/**
	 * The representation is identified by an md5 hash
	 */
	private final ByteBuffer bytes;

	/**
	 * Construct a new instance.
	 * 
	 * @param bytes
	 */
	@DoNotInvoke
	public Representation(ByteBuffer bytes) {
		Preconditions.checkArgument(bytes.capacity() == 16);
		this.bytes = bytes;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof Representation) {
			Representation other = (Representation) obj;
			return getBytes().equals(other.getBytes());
		}
		return false;
	}

	@Override
	public ByteBuffer getBytes() {
		bytes.rewind();
		return bytes;

	}

	@Override
	public int hashCode() {
		return getBytes().hashCode();
	}

	@Override
	public Lock readLock() {
		return Lockables.readLock(this);
	}

	@Override
	public int size() {
		return getBytes().capacity();
	}

	@Override
	public String toString() {
		return Hex.encodeHexString(ByteBuffers.toByteArray(getBytes()));
	}

	@Override
	public Lock writeLock() {
		return Lockables.writeLock(this);
	}

}
