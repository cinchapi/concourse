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
package org.cinchapi.concourse.server.storage;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

import org.cinchapi.concourse.server.concurrent.Lock;
import org.cinchapi.concourse.server.concurrent.Lockable;
import org.cinchapi.concourse.server.concurrent.Lockables;
import org.cinchapi.concourse.server.io.Byteable;
import org.cinchapi.concourse.server.io.ByteableComposite;
import org.cinchapi.concourse.server.io.FileSystem;
import org.cinchapi.concourse.server.io.Syncable;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.hash.BloomFilter;

/**
 * A BlockFilter is a wrapper around a {@link BloomFilter} with methods to make
 * it easier to add one or more {@link Byteable} objects to the filter at a time
 * while abstracting away the notion of funnels, etc. A BlockFilter is
 * associated with each {@link Block} to help efficiently determine if a
 * Revision is contained or not.
 * </p>
 * 
 * @author jnelson
 */
public class BlockFilter implements Syncable, Lockable {

	/**
	 * Create a new BlockFilter with enough capacity for
	 * {@code expectedInsertions}.
	 * <p>
	 * Note that overflowing a BlockFilter with significantly more elements than
	 * specified, will result in its saturation, and a sharp deterioration of
	 * its false positive probability (source:
	 * {@link BloomFilter#create(com.google.common.hash.Funnel, int)})
	 * <p>
	 * 
	 * @param file
	 * @param expectedInsertions
	 * @return the BlockFilter
	 */
	public static BlockFilter create(String file, int expectedInsertions) {
		return new BlockFilter(file, expectedInsertions);
	}

	/**
	 * Return the BlockFilter that is stored on disk in {@code file}.
	 * 
	 * @param file
	 * @return the BlockFilter
	 */
	public static BlockFilter open(String file) {
		try {
			ObjectInput input = new ObjectInputStream(new BufferedInputStream(
					new FileInputStream(FileSystem.openFile(file))));
			BlockFilter filter = (BlockFilter) input.readObject();
			filter.setFile(file);
			input.close();
			return filter;
		}
		catch (IOException e) {
			throw Throwables.propagate(e);
		}
		catch (ClassNotFoundException e) {
			throw Throwables.propagate(e);
		}
	}

	/**
	 * The wrapped bloom filter. This is where the data is actually stored.
	 */
	private final BloomFilter<ByteableComposite> source;

	/**
	 * The file where the content is stored.
	 */
	private String file = null;

	/**
	 * Construct a new instance.
	 * 
	 * @param expectedInsertions
	 */
	private BlockFilter(String file, int expectedInsertions) {
		this.source = BloomFilter.create(ByteableFunnel.INSTANCE,
				expectedInsertions); // uses 3% false positive probability
		this.file = file;
	}

	@Override
	public void sync() {
		Lock lock = readLock();
		try {
			ObjectOutput output = new ObjectOutputStream(
					new BufferedOutputStream(new FileOutputStream(
							FileSystem.openFile(file))));
			output.writeObject(source);
			output.flush();
			output.close();
		}
		catch (IOException e) {
			throw Throwables.propagate(e);
		}
		finally {
			lock.release();
		}
	}

	/**
	 * Return true if an element made up of {@code byteables} might have been
	 * put in this filter or false if this is definitely not the case.
	 * 
	 * @param byteables
	 * @return {@code true} if {@code byteables} might exist
	 */
	public boolean mightContain(Byteable... byteables) {
		Lock lock = readLock();
		try {
			return source.put(ByteableComposite.create(byteables));
		}
		finally {
			lock.release();
		}
	}

	/**
	 * <p>
	 * <strong>Copied from {@link BloomFilter#put(Object)}.</strong>
	 * </p>
	 * Puts {@link byteables} into this BlockFilter as a single element.
	 * Ensures that subsequent invocations of {@link #mightContain(Byteable...)}
	 * with the same elements will always return true.
	 * 
	 * @param byteables
	 * @return {@code true} if the filter's bits changed as a result of this
	 *         operation. If the bits changed, this is definitely the first time
	 *         {@code byteables} have been added to the filter. If the bits
	 *         haven't changed, this might be the first time they have been
	 *         added. Note that put(t) always returns the opposite result to
	 *         what mightContain(t) would have returned at the time it is
	 *         called.
	 */
	public boolean put(Byteable... byteables) {
		Lock lock = writeLock();
		try {
			return source.put(ByteableComposite.create(byteables));
		}
		finally {
			lock.release();
		}
	}

	@Override
	public Lock readLock() {
		return Lockables.readLock(this);
	}

	@Override
	public Lock writeLock() {
		return Lockables.writeLock(this);
	}

	/**
	 * Set {@link #file} if it was not set in the constructor. This should only
	 * be called when deserializing the BlockFilter.
	 * 
	 * @param file
	 */
	private void setFile(String file) {
		Preconditions.checkState(this.file == null);
		this.file = file;
	}
}
