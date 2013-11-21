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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.cinchapi.concourse.server.concurrent.Token;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TObject;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

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

	private static final int INITIAL_CAPACITY = 10;

	private final List<VersionExpectation> expectedVersions = Lists
			.newArrayListWithExpectedSize(INITIAL_CAPACITY);

	/**
	 * Construct a new instance.
	 * 
	 * @param transportable
	 * @param destination - must be a {@link VersionGetter}
	 */
	protected AtomicOperation(Limbo transportable, PermanentStore destination) {
		super(new Queue(INITIAL_CAPACITY), destination);
		Preconditions.checkArgument(destination instanceof VersionGetter);
	}

	@Override
	public boolean add(String key, TObject value, long record) {
		expectedVersions.add(new KeyInRecordVersionExpectation(key, record));
		return super.add(key, value, record);
	}

	@Override
	public Map<Long, String> audit(long record) {
		expectedVersions.add(new RecordVersionExpectation(record));
		return super.audit(record);
	}

	@Override
	public Map<Long, String> audit(String key, long record) {
		expectedVersions.add(new KeyInRecordVersionExpectation(key, record));
		return super.audit(key, record);
	}

	@Override
	public Set<String> describe(long record) {
		expectedVersions.add(new RecordVersionExpectation(record));
		return super.describe(record);
	}

	@Override
	public Set<String> describe(long record, long timestamp) {
		expectedVersions.add(new RecordVersionExpectation(record, timestamp));
		return super.describe(record, timestamp);
	}

	@Override
	public Set<TObject> fetch(String key, long record) {
		expectedVersions.add(new KeyInRecordVersionExpectation(key, record));
		return super.fetch(key, record);
	}

	@Override
	public Set<TObject> fetch(String key, long record, long timestamp) {
		expectedVersions.add(new KeyInRecordVersionExpectation(key, record,
				timestamp));
		return super.fetch(key, record, timestamp);
	}

	@Override
	public Set<Long> find(long timestamp, String key, Operator operator,
			TObject... values) {
		expectedVersions.add(new KeyVersionExpectation(key, timestamp));
		return super.find(timestamp, key, operator, values);
	}

	@Override
	public Set<Long> find(String key, Operator operator, TObject... values) {
		expectedVersions.add(new KeyVersionExpectation(key));
		return super.find(key, operator, values);
	}

	@Override
	public boolean ping(long record) {
		expectedVersions.add(new RecordVersionExpectation(record));
		return super.ping(record);
	}

	@Override
	public boolean remove(String key, TObject value, long record) {
		expectedVersions.add(new KeyInRecordVersionExpectation(key, record));
		return super.remove(key, value, record);
	}

	@Override
	public void revert(String key, long record, long timestamp) {
		// TODO: remove this method from the engine...it should only be exposed
		// in the public API and the server should use an AtomicOperation to
		// accomplish it
	}

	@Override
	public Set<Long> search(String key, String query) {
		expectedVersions.add(new KeyVersionExpectation(key));
		return super.search(key, query);
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

	@Override
	public boolean verify(String key, TObject value, long record) {
		expectedVersions.add(new KeyInRecordVersionExpectation(key, record));
		return super.verify(key, value, record);
	}

	@Override
	public boolean verify(String key, TObject value, long record, long timestamp) {
		expectedVersions.add(new KeyInRecordVersionExpectation(key, record,
				timestamp));
		return super.verify(key, value, record, timestamp);
	}

	/**
	 * The base class for those that determine and stores the expected version
	 * of a record and and/or key and/or timestamp in {@link #destination}. A
	 * VersionExpectation should be stored whenever a read/write occurs in the
	 * AtomicOperation, so that we can check to see if any versions have changed
	 * when we go to commit.
	 * 
	 * @author jnelson
	 */
	private abstract class VersionExpectation {
		// NOTE: This class does not define hashCode() or equals() because the
		// defaults are the desired behaviour.

		/**
		 * The Token that corresponds to the data components that were used to
		 * generate this VersionExpectation.
		 */
		private final Token token;

		/**
		 * OPTIONAL parameter that exists if the VersionExpectation
		 * was generated from a historical read.
		 */
		private final long timestamp;

		/**
		 * OPTINAL parameter that exists iff {@link #timestamp} ==
		 * {@link Versioned#NO_VERSION} since since data returned from a
		 * historical read won't change with additional writes.
		 */
		private final long expectedVersion;

		/**
		 * Construct a new instance.
		 * 
		 * @param token
		 * @param timestamp
		 * @param expectedVersion
		 */
		protected VersionExpectation(Token token, long timestamp,
				long expectedVersion) {
			Preconditions
					.checkState((timestamp != Versioned.NO_VERSION && expectedVersion == Versioned.NO_VERSION) || true);
			this.token = token;
			this.timestamp = timestamp;
			this.expectedVersion = expectedVersion;
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
		 * Return the key, if it exists.
		 * 
		 * @return the key
		 * @throws UnsupportedOperationException
		 */
		public abstract String getKey() throws UnsupportedOperationException;

		/**
		 * Return the record, if it exists.
		 * 
		 * @return the record
		 * @throws UnsupportedOperationException
		 */
		public abstract long getRecord() throws UnsupportedOperationException;

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
			boolean replaceInClause = false;
			sb.append("Expecting version " + expectedVersion + "for '");
			try {
				sb.append(getKey() + " IN ");
			}
			catch (UnsupportedOperationException e) {/* ignore */}
			try {
				sb.append(getRecord());
			}
			catch (UnsupportedOperationException e) {
				/* ignore exception */
				replaceInClause = true;
			}
			if(timestamp != Versioned.NO_VERSION) {
				sb.append(" AT " + timestamp);
			}
			sb.append("'");
			String string = sb.toString();
			if(replaceInClause) {
				string.replace(" IN ", "");
			}
			return string;
		}
	}

	/**
	 * A VersionExpectation for a read that touches an entire record (i.e.
	 * describe, audit, etc).
	 * 
	 * @author jnelson
	 */
	private final class RecordVersionExpectation extends VersionExpectation {

		private final long record;

		/**
		 * Construct a new instance.
		 * 
		 * @param token
		 * @param timestamp
		 * @param expectedVersion
		 */
		public RecordVersionExpectation(long record) {
			super(Token.wrap(record), Versioned.NO_VERSION,
					((VersionGetter) destination).getVersion(record));
			this.record = record;
		}

		/**
		 * Construct a new instance.
		 * 
		 * @param record
		 * @param timestamp
		 */
		public RecordVersionExpectation(long record, long timestamp) {
			super(Token.wrap(record), timestamp, Versioned.NO_VERSION);
			this.record = record;
		}

		@Override
		public String getKey() throws UnsupportedOperationException {
			throw new UnsupportedOperationException();
		}

		@Override
		public long getRecord() throws UnsupportedOperationException {
			return record;
		}

	}

	/**
	 * A VersionExpectation for a read that touches an entire key (i.e.
	 * find, search, etc).
	 * 
	 * @author jnelson
	 */
	private final class KeyVersionExpectation extends VersionExpectation {

		private final String key;

		/**
		 * Construct a new instance.
		 * 
		 * @param token
		 * @param timestamp
		 * @param expectedVersion
		 */
		public KeyVersionExpectation(String key) {
			super(Token.wrap(key), Versioned.NO_VERSION,
					((VersionGetter) destination).getVersion(key));
			this.key = key;
		}

		/**
		 * Construct a new instance.
		 * 
		 * @param key
		 * @param timestamp
		 */
		public KeyVersionExpectation(String key, long timestamp) {
			super(Token.wrap(key), timestamp, Versioned.NO_VERSION);
			this.key = key;
		}

		@Override
		public String getKey() throws UnsupportedOperationException {
			return key;
		}

		@Override
		public long getRecord() throws UnsupportedOperationException {
			throw new UnsupportedOperationException();
		}
	}

	/**
	 * A VersionExpectation for a read or write that touches a key IN a record
	 * (i.e. fetch, verify, etc).
	 * 
	 * @author jnelson
	 */
	private final class KeyInRecordVersionExpectation extends
			VersionExpectation {

		private final long record;
		private final String key;

		/**
		 * Construct a new instance.
		 * 
		 * @param token
		 * @param timestamp
		 * @param expectedVersion
		 */
		protected KeyInRecordVersionExpectation(String key, long record) {
			super(Token.wrap(key, record), Versioned.NO_VERSION,
					((VersionGetter) destination).getVersion(key, record));
			this.key = key;
			this.record = record;
		}

		/**
		 * Construct a new instance. NEVER use this constructor for a write
		 * operation.
		 * 
		 * @param key
		 * @param record
		 * @param timestamp
		 */
		protected KeyInRecordVersionExpectation(String key, long record,
				long timestamp) {
			super(Token.wrap(key, record), timestamp, Versioned.NO_VERSION);
			this.key = key;
			this.record = record;
		}

		@Override
		public String getKey() throws UnsupportedOperationException {
			return key;
		}

		@Override
		public long getRecord() throws UnsupportedOperationException {
			return record;
		}

	}

}
