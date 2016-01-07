/*
 * Copyright (c) 2013-2016 Cinchapi Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse.server;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Set;

import javax.annotation.Nullable;

import ch.qos.logback.classic.Level;

import com.cinchapi.concourse.Constants;
import com.cinchapi.concourse.annotate.NonPreference;
import com.cinchapi.concourse.config.ConcourseServerPreferences;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.util.Networking;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;

/**
 * Contains configuration and state that must be accessible to various parts of
 * the Server.
 * 
 * @author Jeff Nelson
 */
public final class GlobalState extends Constants {
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
     * The amount of memory that is allocated to the Concourse Server JVM.
     * Concourse requires a minimum heap size of 256MB to start, but much
     * more is recommended to ensure that read and write operations avoid
     * expensive disk seeks where possible. Concourse generally sets both
     * the initial and maximum heap sizes to the specified value, so there
     * must be enough system memory available for Concourse Server to start.
     */
    public static long HEAP_SIZE = 1024 * 1024 * 1024; // NOTE: This is handled
                                                       // by Tanuki before the
                                                       // server starts

    /**
     * The listener port (1-65535) for HTTP/S connections. Choose a
     * port between 49152 and 65535 to minimize the possibility of conflicts
     * with other services on this host. A value of 0 indicates that the
     * Concourse HTTP Server is disabled.
     */
    public static int HTTP_PORT = 0;

    /**
     * The default environment that is automatically loaded when the server
     * starts and is used whenever a client does not specify an environment for
     * its connection.
     */
    public static String DEFAULT_ENVIRONMENT = "default";

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
        ConcourseServerPreferences config;
        try {
            String devPrefs = "conf" + File.separator + "concourse.prefs.dev";
            String defaultPrefs = "conf" + File.separator + "concourse.prefs";
            if(FileSystem.hasFile(devPrefs)) {
                config = ConcourseServerPreferences.open(devPrefs);
                PREFS_FILE_PATH = FileSystem.expandPath(devPrefs);
            }
            else {
                config = ConcourseServerPreferences.open(defaultPrefs);
                PREFS_FILE_PATH = FileSystem.expandPath(defaultPrefs);
            }
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

            SHUTDOWN_PORT = config.getInt("shutdown_port",
                    Networking.getCompanionPort(CLIENT_PORT, 2));

            JMX_PORT = config.getInt("jmx_port", JMX_PORT);

            HEAP_SIZE = config.getSize("heap_size", HEAP_SIZE);

            HTTP_PORT = config.getInt("http_port", HTTP_PORT);

            LOG_LEVEL = Level.valueOf(config.getString("log_level",
                    LOG_LEVEL.toString()));

            ENABLE_CONSOLE_LOGGING = config.getBoolean(
                    "enable_console_logging", ENABLE_CONSOLE_LOGGING);
            if(!ENABLE_CONSOLE_LOGGING) {
                ENABLE_CONSOLE_LOGGING = Boolean
                        .parseBoolean(System
                                .getProperty(
                                        "com.cinchapi.concourse.server.logging.console",
                                        "false"));
            }

            DEFAULT_ENVIRONMENT = config.getString("default_environment",
                    DEFAULT_ENVIRONMENT);
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

    /**
     * The file which contains the credentials used by the
     * {@link com.cinchapi.concourse.security.AccessManager}.
     * This file is typically located in the root of the server installation.
     */
    @NonPreference
    public static String ACCESS_FILE = ".access";

    /**
     * The name of the cookie where the HTTP auth token is stored.
     */
    @NonPreference
    public static String HTTP_AUTH_TOKEN_COOKIE = "concoursedb_auth_token";

    /**
     * The name of the header where the HTTP auth token may be stored.
     */
    @NonPreference
    public static String HTTP_AUTH_TOKEN_HEADER = "X-Auth-Token";

    /**
     * The name of the attribute where the {@link AccessToken} component of an
     * AuthToken is temporarily stored for each HTTP Request. This information
     * is used for validation after URL rewriting occurs.
     */
    @NonPreference
    public static final String HTTP_ACCESS_TOKEN_ATTRIBUTE = "com.cinchapi.concourse.server.http.AccessTokenAttribute";

    /**
     * The name of the attribute where the environment component of an
     * AuthToken is temporarily stored for each HTTP Request. This information
     * is used for validation after URL rewriting occurs.
     */
    @NonPreference
    public static final String HTTP_ENVIRONMENT_ATTRIBUTE = "com.cinchapi.concourse.server.http.EnvironmentAttribute";

    /**
     * The name of the attribute where the fingerprint component of an AuthToken
     * is temporarily stored for each HTTP Request. This information is to
     * prevent session hijacking and session fixation.
     */
    @NonPreference
    public static final String HTTP_FINGERPRINT_ATTRIBUTE = "com.cinchapi.concourse.server.http.FingerprintAttribute";

    /**
     * The name of the attribute that is used to signal that an HTTP request
     * requires authentication.
     */
    @NonPreference
    public static final String HTTP_REQUIRE_AUTH_ATTRIBUTE = "com.cinchapi.concourse.server.http.RequireAuthAttribute";

    /**
     * The name of the cookie where the HTTP transaction token is stored.
     */
    @NonPreference
    public static final String HTTP_TRANSACTION_TOKEN_COOKIE = "concoursedb_transaction_token";

    /**
     * The name of the attribute where the transaction token is temporarily
     * stored for each HTTP request.
     */
    @NonPreference
    public static final String HTTP_TRANSACTION_TOKEN_ATTRIBUTE = "com.cinchapi.concourse.server.http.TransactionTokenAttribute";

    /**
     * The path to the underlying file from which the preferences are extracted.
     * This value is set in the static initialization block.
     */
    @NonPreference
    @Nullable
    private static String PREFS_FILE_PATH;

    // ========================================================================

    /**
     * Return the path to the underlying file from which the preferences are
     * extracted.
     * 
     * @return the absolute path to the prefs file
     */
    @Nullable
    public static String getPrefsFilePath() {
        return PREFS_FILE_PATH;
    }

}
