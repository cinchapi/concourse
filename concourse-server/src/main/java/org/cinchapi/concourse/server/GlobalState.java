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

import org.cinchapi.concourse.config.ConcourseConfiguration;
import ch.qos.logback.classic.Level;

import com.google.common.base.Throwables;
import com.google.common.collect.Sets;

/**
 * Contains configuration and state that must be accessible to various parts of
 * the Server.
 * 
 * @author jnelson
 */
public final class GlobalState {

	/**
	 * A flag to indicate if the program is running from Eclipse. This flag has
	 * a value of {@code true} if the JVM is launched with the
	 * {@code -Declipse=true} flag.
	 */
	private static final boolean RUNNING_FROM_ECLIPSE = System
			.getProperty("eclipse") != null
			&& System.getProperty("eclipse").equals("true") ? true : false;

	/* ***************************** CONFIG ******************************** */
	private static final ConcourseConfiguration config = ConcourseConfiguration
			.loadConfig("conf" + File.separator + "concourse.prefs");

	/**
	 * The absolute path to the directory where Concourse stores permanent data
	 * on disk.
	 */
	public static final String DATABASE_DIRECTORY = config.getString(
			"database_directory", System.getProperty("user.home")
					+ File.separator + "concourse" + File.separator + "db");

	/**
	 * The absolute path to the directory where Concourse stores buffer data on
	 * disk.
	 */
	public static final String BUFFER_DIRECTORY = config.getString(
			"buffer_directory", System.getProperty("user.home")
					+ File.separator + "concourse" + File.separator + "buffer");

	/**
	 * The size of a single page in the {@link Buffer}. By using multiple Pages,
	 * the Buffer can localize its locking when performing reads and writes.
	 * When choosing a Page size, seek to balance the potential increased
	 * throughput that smaller pages may produce with the potential for less
	 * fragmented data storage that larger pages may produce.
	 */
	public static final int BUFFER_PAGE_SIZE = (int) config.getSize(
			"buffer_page_size", 8192);

	/**
	 * The port that the server listens on to know when to initiate a graceful
	 * shutdown.
	 */
	public static final int SHUTDOWN_PORT = config
			.getInt("shutdown_port", 3434);

	/**
	 * <p>
	 * The amount of runtime information logged by the system. The options below
	 * are listed from least to most verbose. In addition to the indicated types
	 * of information, each level also logs the information for each less
	 * verbose level (i.e. ERROR only prints error messages, but INFO prints
	 * info, warn and error messages).
	 * </p>
	 * <p>
	 * <ul>
	 * <li><strong>ERROR</strong>: critical information when the system reaches
	 * a potentially fatal state and may not operate normally.</li>
	 * <li><strong>WARN</strong>: useful information when the system reaches a
	 * less than ideal state but can continue to operate normally.</li>
	 * <li><strong>INFO</strong>: status information about the system that can
	 * be used for sanity checking.</li>
	 * <li><strong>DEBUG</strong>: detailed information about the system that
	 * can be used to diagnose bugs.</li>
	 * </ul>
	 * </p>
	 * <p>
	 * Logging is important, but may cause performance degradation. Only use the
	 * DEBUG level for staging environments or instances when detailed
	 * information to diagnose a bug. Otherwise use the WARN or INFO levels.
	 * </p>
	 */
	public static final Level LOG_LEVEL = Level.valueOf(config.getString(
			"log_level", "INFO"));

	/**
	 * Whether log messages should also be printed to the console.
	 */
	public static final boolean ENABLE_CONSOLE_LOGGING = config.getBoolean(
			"enable_console_logging", RUNNING_FROM_ECLIPSE ? true : false);

	/* ************************************************************************ */
	public static final Set<String> STOPWORDS = Sets.newHashSet();
	static {
		try {
			BufferedReader reader = new BufferedReader(new FileReader("conf"
					+ File.separator + "stopwords.txt"));
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

}
