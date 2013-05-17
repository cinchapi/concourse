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

import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.cinchapi.common.lock.Lock;
import com.cinchapi.common.lock.Lockable;
import com.cinchapi.common.lock.Lockables;
import com.cinchapi.concourse.io.ByteSized;

/**
 * A {@link Container} that is externally {@link Lockable}.
 * 
 * @author jnelson
 */
abstract class LockableContainer<K extends ByteSized, V extends Containable> extends
		Container<K, V> implements Lockable {

	private final transient ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

	/**
	 * Construct a new instance.
	 * 
	 * @param key
	 */
	LockableContainer(K key) {
		super(key);
	}

	/**
	 * Construct a new instance. Use this constructor when
	 * reading and reconstructing from a file. This method assumes that
	 * {@code bytes} was generated using {@link #getBytes()}.
	 * 
	 * @param bytes
	 */
	LockableContainer(ByteBuffer bytes) {
		super(bytes);
	}

	@Override
	public Lock readLock() {
		return Lockables.readLock(this, lock);
	}

	@Override
	public Lock writeLock() {
		return Lockables.writeLock(this, lock);
	}

}
