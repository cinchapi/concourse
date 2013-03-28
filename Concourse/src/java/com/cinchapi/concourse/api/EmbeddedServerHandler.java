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
package com.cinchapi.concourse.api;

import com.cinchapi.concourse.config.ConcourseConfiguration;
import com.cinchapi.concourse.db.Engine;

/**
 * 
 * 
 * @author jnelson
 */
class EmbeddedConcourse extends Concourse {

	private final Engine engine;

	/**
	 * Construct a new instance.
	 * 
	 * @param prefs
	 */
	public EmbeddedConcourse(ConcourseConfiguration prefs) {
		engine = Engine.start(prefs);
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				engine.shutdown();
			}
		});
	}

}
