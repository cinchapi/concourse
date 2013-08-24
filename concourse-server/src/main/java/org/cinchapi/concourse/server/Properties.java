/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013 Jeff Nelson, Cinchapi Software Collective
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.cinchapi.concourse.server;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Set;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.cinchapi.common.annotate.UtilityClass;
import org.cinchapi.common.configuration.Configurations;

import com.google.common.base.Throwables;
import com.google.common.collect.Sets;

/**
 * Contains constant variables used throughout this project.
 * 
 * @author jnelson
 */
@UtilityClass
public final class Properties {

	/**
	 * The name of the file that contains configuration information.
	 */
	private static final String configFileName = "opts" + File.separator
			+ "concourse.opts";

	/**
	 * A handler that provides an interface to the file that contains
	 * configuration information.
	 */
	public static PropertiesConfiguration config = Configurations
			.loadPropertiesConfiguration(configFileName);

	/**
	 * The path to the directory where Concourse should store data.
	 * This directory is relative to the Concourse install directory.
	 */
	public static final String DATA_HOME = config.getString("DATA_HOME",
			File.separator + "var" + File.separator + "lib" + File.separator
					+ "concourse");

	/**
	 * The number of bytes available for write buffering. A larger buffer allows
	 * faster writes at the expense of slower reads. A smaller buffer has the
	 * inverse affect.
	 */
	public static final int BUFFER_SIZE_IN_BYTES = config.getInt(
			"BUFFER_SIZE_IN_BYTES", 5242880);

	/**
	 * The minimum number of characters to index for searches. This value
	 * is usually equal to the number of characters a user must enter in
	 * an autocomplete field before seeing suggestions or results. Smaller
	 * values allow more granular searches at the expense of larger index
	 * sizes.
	 */
	public static final int SEARCH_INDEX_GRANULARITY = config.getInt(
			"SEARCH_INDEX_GRANULARITY", 3);

	/**
	 * A collection of <a
	 * href="http://en.wikipedia.org/wiki/Stop_words">stopwords</a> that are
	 * used to make search indexing more efficient.
	 */
	public static final Set<String> STOPWORDS = Sets.newHashSet();
	static {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(
					"opts/stopwords.txt"));
			String line = null;
			while ((line = reader.readLine()) != null) {
				STOPWORDS.add(line);
			}
			reader.close();
		}
		catch (IOException e) {
			throw Throwables.propagate(e);
		}
	}

	private Properties() {/* utility-class */}

}