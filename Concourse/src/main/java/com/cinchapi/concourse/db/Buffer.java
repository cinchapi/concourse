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

import java.util.Set;

import com.cinchapi.concourse.Kernel;
import com.cinchapi.concourse.Operator;

/**
 * 
 * 
 * @author jnelson
 */
class Buffer implements Kernel{

	/* (non-Javadoc)
	 * @see com.cinchapi.concourse.services.ConcourseService#add(long, java.lang.String, java.lang.Object)
	 */
	@Override
	public boolean add(long record, String field, Object value) {
		// TODO Auto-generated method stub
		return false;
	}

	/* (non-Javadoc)
	 * @see com.cinchapi.concourse.services.ConcourseService#describe(long)
	 */
	@Override
	public Set<String> describe(long record) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.cinchapi.concourse.services.ConcourseService#describe(long, long)
	 */
	@Override
	public Set<String> describe(long record, long timestamp) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.cinchapi.concourse.services.ConcourseService#exists(long, java.lang.String, java.lang.Object)
	 */
	@Override
	public boolean does(String field, Object value, long record) {
		// TODO Auto-generated method stub
		return false;
	}

	/* (non-Javadoc)
	 * @see com.cinchapi.concourse.services.ConcourseService#exists(long, java.lang.String, java.lang.Object, long)
	 */
	@Override
	public boolean does(String field, Object value, long record,
			long timestamp) {
		// TODO Auto-generated method stub
		return false;
	}

	/* (non-Javadoc)
	 * @see com.cinchapi.concourse.services.ConcourseService#fetch(long, java.lang.String)
	 */
	@Override
	public Set<Object> fetch(long record, String field) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.cinchapi.concourse.services.ConcourseService#fetch(long, java.lang.String, long)
	 */
	@Override
	public Set<Object> fetch(long record, String field, long timestamp) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.cinchapi.concourse.services.ConcourseService#history(long)
	 */
	@Override
	public void audit(long record) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see com.cinchapi.concourse.services.ConcourseService#history(long, java.lang.String)
	 */
	@Override
	public void audit(long record, String field) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see com.cinchapi.concourse.services.ConcourseService#query(long, java.lang.String, com.cinchapi.concourse.Operator, java.lang.Object[])
	 */
	@Override
	public Set<Long> query(long timestamp, String field, Operator operator,
			Object[] values) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.cinchapi.concourse.services.ConcourseService#query(java.lang.String, com.cinchapi.concourse.Operator, java.lang.Object[])
	 */
	@Override
	public Set<Long> query(String field, Operator operator, Object... values) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.cinchapi.concourse.services.ConcourseService#remove(long, java.lang.String, java.lang.Object)
	 */
	@Override
	public boolean remove(long record, String field, Object value) {
		// TODO Auto-generated method stub
		return false;
	}

	/* (non-Javadoc)
	 * @see com.cinchapi.concourse.services.ConcourseService#revert(long, java.lang.String, long)
	 */
	@Override
	public boolean revert(long record, String field, long timestamp) {
		// TODO Auto-generated method stub
		return false;
	}

	/* (non-Javadoc)
	 * @see com.cinchapi.concourse.services.ConcourseService#set(long, java.lang.String, java.lang.Object)
	 */
	@Override
	public boolean set(long record, String field, Object value) {
		// TODO Auto-generated method stub
		return false;
	}

}
