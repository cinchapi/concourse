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
package com.cinchapi.concourse.client;

import com.cinchapi.concourse.config.ConcourseConfiguration;

/**
 * 
 * 
 * @author jnelson
 */
public class ConcourseController {
	
	public static final String PREFS_FILE = "concourse.prefs";

	/**
	 * Start an embedded Concourse database server, which requires the
	 * presence of a {@value #PREFS_FILE} configuration file in the working
	 * directory.
	 * 
	 * @return the handler for the embedded server
	 */
	public static EmbeddedController forEmbeddedServer() {
		ConcourseConfiguration prefs = ConcourseConfiguration
				.fromFile(PREFS_FILE);
		return new EmbeddedController(prefs);
	}

	// TODO forRemoteServer() forEmbeddedServer()

}
