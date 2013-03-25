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
package com.cinchapi.concourse.internal;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;

import com.cinchapi.common.Strings;
import com.cinchapi.common.io.IterableByteSequences;
import com.cinchapi.concourse.exception.ConcourseRuntimeException;
import com.cinchapi.concourse.io.ByteSized;
import com.cinchapi.concourse.io.ByteSizedCollections;
import com.cinchapi.concourse.io.Persistable;

/**
 * An index that is stored to disk. The index maps a key to a {@link ByteSized}
 * value.
 * 
 * @author jnelson
 * @param <I>
 *            - the identifier for the index
 * @param <K>
 *            - the key used in the index
 * @param <V>
 *            - the ByteSized value in the index
 */
abstract class PersistableIndex<I, K, V extends ByteSized> implements
		IterableByteSequences,
		Persistable {

	protected final Map<K, V> components;
	protected transient final String filename;
	protected transient final I identifier;
	protected transient final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private final Logger log = getLogger();

	protected PersistableIndex(String filename, I identifier,
			Map<K, V> components) {
		this.filename = filename;
		this.identifier = identifier;
		this.components = components;
	}

	@Override
	public final byte[] getBytes() {
		lock.writeLock().lock();
		try {
			return ByteSizedCollections.toByteArray(components.values());
		}
		finally {
			lock.writeLock().unlock();
		}
	}

	@Override
	public final ByteSequencesIterator iterator() {
		lock.writeLock().lock();
		try {
			return IterableByteSequences.ByteSequencesIterator.over(getBytes());
		}
		finally {
			lock.writeLock().unlock();
		}
	}

	@Override
	public final int size() {
		lock.readLock().lock();
		try {
			int size = 0;
			Iterator<V> it = components.values().iterator();
			while (it.hasNext()) {
				size += it.next().size();
			}
			return size;
		}
		finally {
			lock.readLock().unlock();
		}
	}

	@Override
	public String toString() {
		return Strings.toString(this);
	}

	@Override
	public final void writeTo(FileChannel channel) throws IOException {
		lock.writeLock().lock();
		try {
			Writer.write(this, channel);
		}
		finally {
			lock.writeLock().unlock();
		}
	}

	/**
	 * Sync changes made to the row back to disk.
	 */
	void fsync() {
		lock.writeLock().lock();
		try {
			FileChannel channel = new FileOutputStream(filename).getChannel();
			channel.position(0);
			writeTo(channel);
		}
		catch (IOException e) {
			log.error("An error occured while trying to fsync {} {} to {}: {}",
					getClass().getName(), identifier, filename, e);
			throw new ConcourseRuntimeException(e);
		}
		finally {
			lock.writeLock().unlock();
		}
	}

	/**
	 * Return a <strong>static</strong> {@link Logger}.
	 * 
	 * @return the logger
	 */
	protected abstract Logger getLogger();

}
