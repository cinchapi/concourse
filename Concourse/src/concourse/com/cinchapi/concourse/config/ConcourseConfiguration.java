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
package com.cinchapi.concourse.config;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import com.cinchapi.concourse.exception.ConcourseRuntimeException;

/**
 * Handles Concourse configuration files.
 * 
 * @author jnelson
 */
public class ConcourseConfiguration {

	/**
	 * Return a configuration based on the contents of {@code file}.
	 * 
	 * @param file
	 * @return the configuration
	 */
	public static ConcourseConfiguration fromFile(String file) {
		try {
			return new ConcourseConfiguration(file);
		}
		catch (ConfigurationException e) {
			throw new ConcourseRuntimeException(e);
		}
	}

	private final PropertiesConfiguration config;

	/**
	 * Construct a new instance.
	 * 
	 * @param file
	 * @throws ConfigurationException
	 */
	private ConcourseConfiguration(String file) throws ConfigurationException {
		this.config = new PropertiesConfiguration(file);
	}

	/**
	 * Get a string associated with the given configuration key. If the key
	 * doesn't map to an existing object, the default value is returned.
	 * 
	 * @param key
	 * @param defaultValue
	 * @return The associated string if key is found and has valid format,
	 *         default value otherwise.
	 */
	public String getString(PrefsKey key, String defaultValue) {
		return config.getString(key.toString(), defaultValue);
	}

	/**
	 * Get a int associated with the given configuration key. If the key doesn't
	 * map to an existing object, the default value is returned.
	 * 
	 * @param key
	 * @param defaultValue
	 * @return The associated int.
	 */
	public int getInt(PrefsKey key, int defaultValue) {
		return config.getInt(key.toString(), defaultValue);
	}

	/**
	 * Get a double associated with the given configuration key. If the key
	 * doesn't map to an existing object, the default value is returned.
	 * 
	 * @param key
	 * @param defaultValue
	 * @return The associated double.
	 */
	public double getDouble(PrefsKey key, double defaultValue) {
		return config.getDouble(key.toString(), defaultValue);
	}

	/**
	 * Marker interface for configuration keys so there is no need to
	 * specify raw strings when retrieving values. The toString() method of
	 * the implementing class is used to determine the lookup key.
	 * 
	 * @author jnelson
	 */
	public interface PrefsKey {}
}
