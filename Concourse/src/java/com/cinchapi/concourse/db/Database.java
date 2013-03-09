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
package com.cinchapi.concourse.db;

import java.util.Iterator;
import java.util.Set;

import com.cinchapi.concourse.api.ConcourseService;
import com.cinchapi.concourse.temp.Commit;
import com.cinchapi.concourse.temp.CommitLog;

/**
 * 
 * 
 * @author jnelson
 */
public class Database extends ConcourseService {

	public synchronized void flush(CommitLog commitLog) {
		Iterator<Commit> commiterator = commitLog.getCommits().iterator();
		while(commiterator.hasNext()){
			Commit commit = commiterator.next();
			
		}
	}

	/* (non-Javadoc)
	 * @see com.cinchapi.concourse.api.ConcourseService#addSpi(long, java.lang.String, java.lang.Object)
	 */
	@Override
	protected boolean addSpi(long row, String column, Object value) {
		// TODO Auto-generated method stub
		return false;
	}

	/* (non-Javadoc)
	 * @see com.cinchapi.concourse.api.ConcourseService#describeSpi(long)
	 */
	@Override
	protected Set<String> describeSpi(long row) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.cinchapi.concourse.api.ConcourseService#existsSpi(long, java.lang.String, java.lang.Object)
	 */
	@Override
	protected boolean existsSpi(long row, String column, Object value) {
		// TODO Auto-generated method stub
		return false;
	}

	/* (non-Javadoc)
	 * @see com.cinchapi.concourse.api.ConcourseService#getSpi(long, java.lang.String)
	 */
	@Override
	protected Set<Object> getSpi(long row, String column) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.cinchapi.concourse.api.ConcourseService#removeSpi(long, java.lang.String, java.lang.Object)
	 */
	@Override
	protected boolean removeSpi(long row, String column, Object value) {
		// TODO Auto-generated method stub
		return false;
	}

	/* (non-Javadoc)
	 * @see com.cinchapi.concourse.api.ConcourseService#selectSpi(java.lang.String, com.cinchapi.concourse.api.Queryable.SelectOperator, java.lang.Object[])
	 */
	@Override
	protected Set<Long> selectSpi(String column, SelectOperator operator,
			Object... values) {
		// TODO Auto-generated method stub
		return null;
	}

}
