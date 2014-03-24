/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2014 Jeff Nelson, Cinchapi Software Collective
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

import org.cinchapi.concourse.annotate.NonPreference;
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
    // ========================================================================
    // =========================== SYSTEM METADATA ============================
    /**
     * A flag to indicate if the program is running from Eclipse. This flag has
     * a value of {@code true} if the JVM is launched with the
     * {@code -Declipse=true} flag.
     */
    private static final boolean RUNNING_FROM_ECLIPSE = System
            .getProperty("eclipse") != null
            && System.getProperty("eclipse").equals("true") ? true : false;

    // ========================================================================
    // ============================ PREFERENCES ================================
    /*
     * INSTRUCTIONS FOR ADDING CONFIGURATION PREFERENCES
     * 1. Create the appropriately named static variable and assigned it a
     * default value.
     * 2. Find the PREF READING BLOCK and attempt to read the value from the
     * prefs file, while supplying the variable you made in Step 1 as the
     * defaultValue.
     * 3. Add a placeholder for the new preference to the stock concourse.prefs
     * file in conf/concourse.prefs.
     */

    /**
     * The absolute path to the directory where the Database record and index
     * files are stored. For optimal performance, the Database should be
     * placed on a separate disk partition (ideally a separate physical device)
     * from the buffer_directory.
     */
    public static String DATABASE_DIRECTORY = System.getProperty("user.home")
            + File.separator + "concourse" + File.separator + "db";

    /**
     * The absolute path to the directory where the Buffer data is stored.
     * For optimal write performance, the Buffer should be placed on a
     * separate disk partition (ideally a separate physical device) from
     * the database_directory.
     */
    public static String BUFFER_DIRECTORY = System.getProperty("user.home")
            + File.separator + "concourse" + File.separator + "buffer";

    /**
     * The size for each page in the Buffer. During reads, Buffer pages
     * are individually locked, so it is desirable to have several smaller
     * pages as opposed to few larger ones. Nevertheless, be sure to balance
     * the desire to maximize lock granularity with the risks of having too
     * many open buffer files simultaneously.
     */
    public static int BUFFER_PAGE_SIZE = 8192;

    /**
     * The listener port (1-65535) for client connections. Choose a port between
     * 49152 and 65535 to minimize the possibility of conflicts with other
     * services on this host.
     */
    public static int CLIENT_PORT = 1717;

    /**
     * The port on which the ShutdownRunner listens. Choose a port between
     * 49152 and 65535 to minimize the possibility of conflicts with other
     * services on this host.
     */
    public static int SHUTDOWN_PORT = 3434;

    /**
     * The listener port (1-65535) for management connections via JMX. Choose a
     * port between 49152 and 65535 to minimize the possibility of conflicts
     * with other services on this host.
     */
    public static int JMX_PORT = 9010;

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
    public static Level LOG_LEVEL = Level.INFO;

    /**
     * Whether log messages should also be printed to the console.
     */
    public static boolean ENABLE_CONSOLE_LOGGING = RUNNING_FROM_ECLIPSE ? true
            : false;

    static {
        ConcourseConfiguration config;
        try {
            config = ConcourseConfiguration.loadConfig("conf" + File.separator
                    + "concourse.prefs");
        }
        catch (Exception e) {
            config = null;
        }
        if(config != null) {
            // =================== PREF READING BLOCK ====================
            DATABASE_DIRECTORY = config.getString("database_directory",
                    DATABASE_DIRECTORY);

            BUFFER_DIRECTORY = config.getString("buffer_directory",
                    BUFFER_DIRECTORY);

            BUFFER_PAGE_SIZE = (int) config.getSize("buffer_page_size",
                    BUFFER_PAGE_SIZE);

            CLIENT_PORT = config.getInt("client_port", CLIENT_PORT);

            SHUTDOWN_PORT = config.getInt("shutdown_port", SHUTDOWN_PORT);
            
            JMX_PORT = config.getInt("jmx_port", JMX_PORT);

            LOG_LEVEL = Level.valueOf(config.getString("log_level",
                    LOG_LEVEL.toString()));

            ENABLE_CONSOLE_LOGGING = config.getBoolean(
                    "enable_console_logging", ENABLE_CONSOLE_LOGGING);
            // =================== PREF READING BLOCK ====================
        }
    }

    /**
     * The list of words that are omitted from search indexes to increase speed
     * and improve space efficiency.
     */
    @NonPreference
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
    // ========================================================================

}
