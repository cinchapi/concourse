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
package com.cinchapi.concourse.store;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cinchapi.concourse.service.ConcourseService;
import com.google.common.collect.Sets;

/**
 * 
 * 
 * @author jnelson
 */
public class Database extends ConcourseService {

	private static Logger log = LoggerFactory.getLogger(Database.class);

	public static Database inDir(String directory) {
		return new Database();
	}

	public synchronized void flush(CommitLog commitLog) {

	}

	@Override
	public synchronized void shutdown() {
		log.info("Successfully shutdown the Database.");

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.cinchapi.concourse.api.ConcourseService#addSpi(long,
	 * java.lang.String, java.lang.Object)
	 */
	@Override
	protected boolean addSpi(String column, Object value, long row) {
		// TODO Auto-generated method stub
		return false;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.cinchapi.concourse.api.ConcourseService#describeSpi(long)
	 */
	@Override
	protected Set<String> describeSpi(long row) {
		return Sets.newHashSet();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.cinchapi.concourse.api.ConcourseService#existsSpi(long,
	 * java.lang.String, java.lang.Object)
	 */
	@Override
	protected boolean existsSpi(String column, Object value, long row) {
		// TODO Auto-generated method stub
		return false;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.cinchapi.concourse.services.ConcourseService#fetchSpi(long,
	 * java.lang.String, long)
	 */
	@Override
	protected Set<Object> fetchSpi(String column, long timestamp, long row) {
		return Sets.newHashSet();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.cinchapi.concourse.api.ConcourseService#selectSpi(java.lang.String,
	 * com.cinchapi.concourse.api.Queryable.SelectOperator, java.lang.Object[])
	 */
	@Override
	protected Set<Long> querySpi(String column, Operator operator,
			Object... values) {
		return Sets.newHashSet();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.cinchapi.concourse.api.ConcourseService#removeSpi(long,
	 * java.lang.String, java.lang.Object)
	 */
	@Override
	protected boolean removeSpi(String column, Object value, long row) {
		return false;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.cinchapi.concourse.service.ConcourseService#sizeOfSpi(java.lang.Long,
	 * java.lang.String)
	 */
	@Override
	protected long sizeOfSpi(String column, Long row) {
		return 0;
	}

}
