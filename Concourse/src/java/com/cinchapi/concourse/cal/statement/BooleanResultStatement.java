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
 * A {@link Statement} that returns a boolean when executed.
 * 
 * @author jnelson
 */
public abstract class BooleanResultStatement implements Statement<BooleanResult> {
	
	public abstract List<BooleanResultStatementMapping> getMappings();


}
