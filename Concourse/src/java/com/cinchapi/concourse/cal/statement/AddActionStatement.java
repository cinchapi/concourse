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
package com.cinchapi.concourse.cal.statement;

import java.util.List;

import com.cinchapi.concourse.cal.result.BooleanResult;

/**
 * 
 * 
 * @author jnelson
 */
public class AddActionStatement extends BooleanResultStatement {

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.cinchapi.concourse.cal.statement.Statement#getAction()
	 */
	@Override
	public Action getAction() {
		return Action.ADD;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.cinchapi.concourse.cal.statement.Statement#execute()
	 */
	@Override
	public BooleanResult execute() {
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.cinchapi.concourse.cal.statement.BooleanResultStatement#getMappings()
	 */
	@Override
	public List<BooleanResultStatementMapping> getMappings() {
		// TODO Auto-generated method stub
		return null;
	}

}
