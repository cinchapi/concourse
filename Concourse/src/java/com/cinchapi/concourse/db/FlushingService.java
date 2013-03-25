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

/**
 * A service that flushes {@link Write} objects from a {@link FlushableService}
 * and is therefore unable to be written to directly.
 * 
 * @author jnelson
 */
public abstract class FlushingService extends ConcourseService {

	/**
	 * Flush the contents of the {@code service}.
	 * 
	 * @param service
	 */
	public abstract void flush(FlushableService service);

	@Override
	protected final boolean addSpi(String column, Object value, long row) {
		return false;
	}

	@Override
	protected final boolean removeSpi(String column, Object value, long row) {
		return false;
	}

}
