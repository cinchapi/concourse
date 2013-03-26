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
package com.cinchapi.concourse;

import java.util.Set;

import com.cinchapi.concourse.config.ConcourseConfiguration;
import com.cinchapi.concourse.db.DataStoreService;
import com.cinchapi.concourse.db.Engine;
import com.cinchapi.concourse.db.Transaction;
import com.cinchapi.concourse.db.TransactionService;
import com.google.common.collect.Sets;

/**
 * <p>
 * Concourse is a schemaless database management system that is designed for
 * applications that have large amounts of sparse data in read and write heavy
 * environments. Concourse comes with automatic indexing, data versioning and
 * support for ACID transactions.
 * 
 * <h2>Intent</h2>
 * Concourse aims to be a service that is easy for developers to deploy, access
 * and scale with minimal tuning, while also being highly optimized for fast
 * read/write operations. With Concourse there is no need to declare any
 * structure up front--no schema, no tables, no keyspaces, no indexes, no column
 * families, etc. Instead, you simply write any value <sup>1</sup> to any column
 * in any row at any time and the necessary structure is created for you on
 * demand. <br>
 * <br>
 * <sup>1</sup> - You cannot write a duplicate value to a cell.
 * <h2>Data Model</h2>
 * Concourse is a big matrix where each row represents a single, canonical
 * object record and each column represents an attribute in the data universe.
 * The intersection of a row and column--a cell--specifies the
 * <strong>values</strong><sup>2</sup> for the relevant attribute on the
 * relevant object.
 * <ul>
 * <li>Each value is versioned by timestamp.<sup>3</sup></li>
 * <li>Each cell sorts its values by timestamp in descending order and also
 * maintains a historical log of revisions.</li>
 * <li>An index of rows sorted by id, an index of columns sorted logically, and
 * a full text index of values are all maintained for optimal reads.</li>
 * </ul>
 * <sup>2</sup> - A cell can simultaneously hold many distinct values and
 * multiple types.<br>
 * <sup>3</sup> - Each value is guaranteed to have a unique timestamp.
 * 
 * <h2>Graph Model</h2>
 * As a matrix, Concourse naturally represents which nodes on a graph are
 * connected to which other nodes: each row and each column corresponds to a
 * node and each value in the cell formed at the intersection of the row and
 * column corresponds to an edge between the corresponding row node and column
 * node on the graph--an edge whose weight is equal to the value.
 * 
 * <h2>Data Processing</h2>
 * Concourse is designed for highly efficient reads and writes.
 * <h4>Writes</h4>
 * Initially all data is written to an append only commit log. The commit log
 * exists in memory and is flushed to disk periodically.
 * <h4>Reads</h4>
 * For reads, Concourse queries its internal database and commit log for the
 * appropriate result sets according to their respective views of the data. The
 * two results sets are resolved by taking their XOR (see
 * {@link Sets#symmetricDifference(Set, Set)} before being returned.
 * 
 * <h2>Additional Notes</h2>
 * <ul>
 * <li>
 * In its present implementation, Concourse can only increase in size (even if
 * data is removed) because every single revision is tracked. In the future,
 * functionality to purge history and therefore reduce the size of the database
 * <em>should</em> be added.</li>
 * </ul>
 * </p>
 * 
 * 
 * @author jnelson
 */
public class Concourse implements DataStoreService, TransactionService {

	/**
	 * Start {@code Concourse}.
	 * 
	 * @return Concourse
	 */
	public static Concourse start() {
		ConcourseConfiguration prefs = ConcourseConfiguration
				.fromFile(PREFS_FILE);
		Engine engine = Engine.start(prefs);
		return new Concourse(engine);
	}

	private static final String PREFS_FILE = "concourse.prefs";

	private final Engine engine;

	/**
	 * Construct a new instance.
	 * 
	 * @param engine
	 */
	private Concourse(Engine engine) {
		this.engine = engine;
	}

	@Override
	public boolean add(String column, Object value, long row) {
		return engine.add(column, value, row);
	}
	
	public boolean addRelation(long fromRow, long toRow, String column){
		return false;
	}

	@Override
	public Set<String> describe(long row) {
		return engine.describe(row);
	}

	@Override
	public boolean exists(long row) {
		return engine.exists(row);
	}

	@Override
	public boolean exists(String column, long row) {
		return engine.exists(column, row);
	}

	@Override
	public boolean exists(String column, Object value, long row) {
		return engine.exists(column, value, row);
	}

	@Override
	public Set<Object> fetch(String column, long row) {
		return engine.fetch(column, row);
	}

	@Override
	public Set<Object> fetch(String column, long timestamp, long row) {
		return engine.fetch(column, timestamp, row);
	}

	@Override
	public Set<Long> query(String column, Operator operator, Object... values) {
		return engine.query(column, operator, values);
	}

	@Override
	public boolean remove(String column, Object value, long row) {
		return engine.remove(column, value, row);
	}

	@Override
	public boolean revert(String column, long timestamp, long row) {
		return engine.revert(column, timestamp, row);
	}

	@Override
	public boolean set(String column, Object value, long row) {
		return engine.set(column, value, row);
	}

	@Override
	public void shutdown() {
		engine.shutdown();
	}

	@Override
	public long sizeOf() {
		return engine.sizeOf();
	}

	@Override
	public long sizeOf(long row) {
		return engine.sizeOf(row);
	}

	@Override
	public long sizeOf(String column, Long row) {
		return engine.sizeOf(column, row);
	}

	@Override
	public Transaction startTransaction() {
		return engine.startTransaction();
	}

	@Override
	public String getTransactionFileName() {
		throw new UnsupportedOperationException("Do not call this method");
	}

}
