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

import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.cinchapi.concourse.server.concurrent.Token;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TObject;

import com.google.common.base.Preconditions;

/**
 * A sequence of reads and writes that all succeed or fail together.
 * 
 * @author jnelson
 */
public class AtomicOperation extends BufferedStore {
	// NOTE: This class does not need to do any locking on operations (until
	// commit time) because it is assumed to be isolated to one thread and the
	// destination is assumed to have its own concurrency control scheme in
	// place.

	// public static AtomicOperation start(PermanentStore destination){
	// return null;
	// }

	private static final int INITIAL_CAPACITY = 10;

	/**
	 * Construct a new instance.
	 * 
	 * @param transportable
	 * @param destination
	 */
	protected AtomicOperation(Limbo transportable, PermanentStore destination) {
		super(new Queue(INITIAL_CAPACITY), destination);
		Preconditions.checkArgument(destination instanceof VersionGetter);
	}

	@Override
	public boolean add(String key, TObject value, long record) {
		// TODO Auto-generated method stub
		return super.add(key, value, record);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.cinchapi.concourse.server.storage.BufferedStore#audit(long)
	 */
	@Override
	public Map<Long, String> audit(long record) {
		// TODO Auto-generated method stub
		return super.audit(record);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.cinchapi.concourse.server.storage.BufferedStore#audit(java.lang.String
	 * , long)
	 */
	@Override
	public Map<Long, String> audit(String key, long record) {
		// TODO Auto-generated method stub
		return super.audit(key, record);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.cinchapi.concourse.server.storage.BufferedStore#describe(long)
	 */
	@Override
	public Set<String> describe(long record) {
		// TODO Auto-generated method stub
		return super.describe(record);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.cinchapi.concourse.server.storage.BufferedStore#describe(long,
	 * long)
	 */
	@Override
	public Set<String> describe(long record, long timestamp) {
		// TODO Auto-generated method stub
		return super.describe(record, timestamp);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.cinchapi.concourse.server.storage.BufferedStore#fetch(java.lang.String
	 * , long)
	 */
	@Override
	public Set<TObject> fetch(String key, long record) {
		// TODO Auto-generated method stub
		return super.fetch(key, record);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.cinchapi.concourse.server.storage.BufferedStore#fetch(java.lang.String
	 * , long, long)
	 */
	@Override
	public Set<TObject> fetch(String key, long record, long timestamp) {
		// TODO Auto-generated method stub
		return super.fetch(key, record, timestamp);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.cinchapi.concourse.server.storage.BufferedStore#find(long,
	 * java.lang.String, org.cinchapi.concourse.thrift.Operator,
	 * org.cinchapi.concourse.thrift.TObject[])
	 */
	@Override
	public Set<Long> find(long timestamp, String key, Operator operator,
			TObject... values) {
		// TODO Auto-generated method stub
		return super.find(timestamp, key, operator, values);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.cinchapi.concourse.server.storage.BufferedStore#find(java.lang.String
	 * , org.cinchapi.concourse.thrift.Operator,
	 * org.cinchapi.concourse.thrift.TObject[])
	 */
	@Override
	public Set<Long> find(String key, Operator operator, TObject... values) {
		// TODO Auto-generated method stub
		return super.find(key, operator, values);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.cinchapi.concourse.server.storage.BufferedStore#ping(long)
	 */
	@Override
	public boolean ping(long record) {
		// TODO Auto-generated method stub
		return super.ping(record);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.cinchapi.concourse.server.storage.BufferedStore#remove(java.lang.
	 * String, org.cinchapi.concourse.thrift.TObject, long)
	 */
	@Override
	public boolean remove(String key, TObject value, long record) {
		// TODO Auto-generated method stub
		return super.remove(key, value, record);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.cinchapi.concourse.server.storage.BufferedStore#revert(java.lang.
	 * String, long, long)
	 */
	@Override
	public void revert(String key, long record, long timestamp) {
		// TODO: remove this method from the engine...it should only be exposed
		// in the public API and the server should use an AtomicOperation to
		// accomplish it
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.cinchapi.concourse.server.storage.BufferedStore#search(java.lang.
	 * String, java.lang.String)
	 */
	@Override
	public Set<Long> search(String key, String query) {
		// TODO Auto-generated method stub
		return super.search(key, query);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.cinchapi.concourse.server.storage.BufferedStore#verify(java.lang.
	 * String, org.cinchapi.concourse.thrift.TObject, long)
	 */
	@Override
	public boolean verify(String key, TObject value, long record) {
		// TODO Auto-generated method stub
		return super.verify(key, value, record);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.cinchapi.concourse.server.storage.BufferedStore#verify(java.lang.
	 * String, org.cinchapi.concourse.thrift.TObject, long, long)
	 */
	@Override
	public boolean verify(String key, TObject value, long record, long timestamp) {
		// TODO Auto-generated method stub
		return super.verify(key, value, record, timestamp);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.cinchapi.concourse.server.storage.Store#start()
	 */
	@Override
	public void start() {
		// TODO Auto-generated method stub

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.cinchapi.concourse.server.storage.Store#stop()
	 */
	@Override
	public void stop() {
		// TODO Auto-generated method stub

	}

	/**
	 * This class determines and stores the expected version of a record and
	 * possibly key and/or timestamp in {@link #destination}. A
	 * VersionExpectation should be stored whenever a read/write occurs in the
	 * AtomicOperation, so that we can check to see if any versions have changed
	 * when we go to commit.
	 * 
	 * @author jnelson
	 */
	private final class VersionExpectation {
		// NOTE: This class does not define hashCode() or equals() because the
		// defaults are the desired behaviour.

		@Nullable
		private final String key;

		private final long record;

		@Nullable
		private final long timestamp;

		@Nullable
		private final long expectedVersion;

		private final Token token;

		/**
		 * Return a VersionExpecation for {@code record}.
		 * 
		 * @param record
		 */
		public VersionExpectation(long record) {
			this(null, record, Versioned.NO_VERSION);
		}

		/**
		 * Return a VersionExpectation for {@code record} AT {@code timestamp}.
		 * <p>
		 * <strong>NOTE:</strong> Never supply a timestamp for historical write
		 * operations (i.e. revert).
		 * </p>
		 * 
		 * @param record
		 * @param timestamp
		 */
		public VersionExpectation(long record, long timestamp) {
			this(null, record, timestamp);
		}

		/**
		 * Return a VersionExpectation for {@code key} IN {@code record}.
		 * Construct a new instance.
		 * 
		 * @param key
		 * @param record
		 */
		public VersionExpectation(String key, long record) {
			this(key, record, Versioned.NO_VERSION);
		}

		/**
		 * This is the default constructor. If none of the parameters are null
		 * this will return a VersionExpectation for {@code key} IN
		 * {@code record} at {@code timestamp}. Otherwise, this will collapse to
		 * one of the cases for the other constructors.
		 * <p>
		 * <strong>NOTE:</strong> Never supply a timestamp for historical write
		 * operations (i.e. revert).
		 * </p>
		 * 
		 * @param key
		 * @param record
		 * @param timestamp
		 */
		public VersionExpectation(@Nullable String key, long record,
				@Nullable long timestamp) {
			this.key = key;
			this.record = record;
			this.timestamp = timestamp;
			if(key != null) {
				this.token = Token.wrap(key, record);
				if(timestamp != Versioned.NO_VERSION) { // case: key IN record
														// AT timestamp
					// There is no expected version when doing a historical
					// read, since the returned data won't change with
					// additional rights.
					this.expectedVersion = Versioned.NO_VERSION;
				}
				else { // case: key IN record
					this.expectedVersion = ((VersionGetter) destination)
							.getVersion(key, record);
				}
			}
			else {
				this.token = Token.wrap(record);
				if(timestamp != Versioned.NO_VERSION) { // case: record AT
														// timestamp
					// There is no expected version when doing a historical
					// read, since the returned data won't change with
					// additional rights.
					this.expectedVersion = Versioned.NO_VERSION;
				}
				else { // case: record
					this.expectedVersion = ((VersionGetter) destination)
							.getVersion(record);
				}
			}
		}

		/**
		 * Return the expected version.
		 * 
		 * @return the expected version
		 */
		public long getExpectedVersion() {
			return expectedVersion;
		}

		/**
		 * Return the key.
		 * 
		 * @return the key.
		 */
		public String getKey() {
			return key;
		}

		/**
		 * Return the record.
		 * 
		 * @return the record
		 */
		public long getRecord() {
			return record;
		}

		/**
		 * Return the token that can be used to grab the appropriate lock over
		 * the data components held within.
		 * 
		 * @return the Token
		 */
		public Token getToken() {
			return token;
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("Expecting version " + expectedVersion + "for '");
			if(key != null) {
				sb.append(key + " IN ");
			}
			sb.append(record);
			if(timestamp != Versioned.NO_VERSION) {
				sb.append(" AT " + timestamp);
			}
			sb.append("'");
			return sb.toString();
		}
	}

}
