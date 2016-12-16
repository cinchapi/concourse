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
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

import javax.annotation.Nullable;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.Constants;
import com.cinchapi.concourse.annotate.NonPreference;
import com.cinchapi.concourse.config.ConcourseServerPreferences;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.plugin.data.WriteEvent;
import com.cinchapi.concourse.util.ByteBuffers;
import com.cinchapi.concourse.util.Networking;
import com.google.common.base.MoreObjects;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import ch.qos.logback.classic.Level;

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
     * This {@link UUID} is used to identify the Concourse instance across host
     * and port changes. This id will be registered locally in the system in
     * data and buffer directory.
     */
    @NonPreference
    @Nullable
    public static UUID SYSTEM_ID = null;

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
     * The listener port (1-65535) for JMX connections. Choose a port between
     * 49152 and 65535 to minimize the possibility of conflicts with other
     * services on this host.
     */
    public static int JMX_PORT = 9010;

    /**
     * The listener port (1-65535) for the management server. Choose a port
     * between
     * 49152 and 65535 to minimize the possibility of conflicts with other
     * services on this host.
     */
    public static int MANAGEMENT_PORT = 9011;

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
     * Determine if the HTTP Server should enable and process preferences
     * related to Cross-Origin Resource Sharing requests.
     */
    public static boolean HTTP_ENABLE_CORS = false;

    /**
     * A comma separated list of default URIs that are permitted to access HTTP
     * endpoints. By default (if enabled), the value of this preference is set
     * to the wildcard character '*' which means that any origin is allowed
     * access. Changing this value to a discrete list will set the default
     * origins that are permitted, but individual endpoints may override this
     * value.
     */
    public static String HTTP_CORS_DEFAULT_ALLOW_ORIGIN = "*";

    /**
     * A comma separated list of default headers that are sent in response to a
     * CORS preflight request to indicate which HTTP headers can be used when
     * making the actual request. By default (if enabled), the value of this
     * preference is set to the wildcard character '*' which means that any
     * headers specified in the preflight request are allowed. Changing this
     * value to a discrete list will set the default headers that are permitted,
     * but individual endpoints may override this value.
     */
    public static String HTTP_CORS_DEFAULT_ALLOW_HEADERS = "*";

    /**
     * A comma separated list of default methods that are sent in response to a
     * CORS preflight request to indicate which HTTP methods can be used when
     * making the actual request. By default (if enabled), the value of this
     * preference is set to the wildcard character '*' which means that any
     * method specified in the preflight request is allowed. Changing this value
     * to a discrete list will set the default methods that are permitted, but
     * individual endpoints may override this value.
     */
    public static String HTTP_CORS_DEFAULT_ALLOW_METHODS = "*";

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
     * The class representation of {@link RemoteInvocationThread}.
     */
    @NonPreference
    public static final Class<?> INVOCATION_THREAD_CLASS = Reflection
            .getClassCasted(
                    "com.cinchapi.concourse.server.plugin.RemoteInvocationThread");

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

            HTTP_ENABLE_CORS = config.getBoolean("http_enable_cors",
                    HTTP_ENABLE_CORS);

            HTTP_CORS_DEFAULT_ALLOW_ORIGIN = config.getString(
                    "http_cors_default_allow_origin",
                    HTTP_CORS_DEFAULT_ALLOW_ORIGIN);

            HTTP_CORS_DEFAULT_ALLOW_HEADERS = config.getString(
                    "http_cors_default_allow_headers",
                    HTTP_CORS_DEFAULT_ALLOW_HEADERS);

            HTTP_CORS_DEFAULT_ALLOW_METHODS = config.getString(
                    "http_cors_default_allow_methods",
                    HTTP_CORS_DEFAULT_ALLOW_METHODS);

            LOG_LEVEL = Level.valueOf(
                    config.getString("log_level", LOG_LEVEL.toString()));

            ENABLE_CONSOLE_LOGGING = config.getBoolean("enable_console_logging",
                    ENABLE_CONSOLE_LOGGING);
            if(!ENABLE_CONSOLE_LOGGING) {
                ENABLE_CONSOLE_LOGGING = Boolean
                        .parseBoolean(System.getProperty(
                                "com.cinchapi.concourse.server.logging.console",
                                "false"));
            }

            DEFAULT_ENVIRONMENT = config.getString("default_environment",
                    DEFAULT_ENVIRONMENT);

            MANAGEMENT_PORT = config.getInt("management_port",
                    Networking.getCompanionPort(CLIENT_PORT, 4));

            SYSTEM_ID = getSystemId();
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
            BufferedReader reader = new BufferedReader(
                    new FileReader("conf" + File.separator + "stopwords.txt"));
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
     * A global {@link BlockingQueue} that is populated with {@link WriteEvent
     * write events} within each environment's {@link Buffer}.
     */
    @NonPreference
    public static LinkedBlockingQueue<WriteEvent> BINARY_QUEUE = new LinkedBlockingQueue<WriteEvent>();

    /**
     * The absolute path to the root of the directory where Concourse Server is
     * installed. This value is set by the start script. When running from
     * Eclipse, this value is set to the launch directory.
     */
    @NonPreference
    public static String CONCOURSE_HOME = MoreObjects.firstNonNull(
            System.getProperty("com.cinchapi.concourse.server.home"),
            System.getProperty("user.dir"));

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

    /**
     * Return the canonical system id based on the storage directories that are
     * configured this this instance.
     * <p>
     * If the system id does not exist, create a new one and store it. If
     * different system ids are stored, return {@code null} to indicate that the
     * system is in an inconsistent state.
     * </p>
     * 
     * @return a {@link UUID} that represents the system id
     */
    private static UUID getSystemId() {
        String relativeFileName = ".id";
        Path bufferId = Paths.get(BUFFER_DIRECTORY, relativeFileName);
        Path databaseId = Paths.get(DATABASE_DIRECTORY, relativeFileName);
        List<String> files = Lists.newArrayList(bufferId.toString(),
                databaseId.toString());
        boolean hasBufferId = false;
        boolean hasDatabaseId = false;
        if((hasBufferId = FileSystem.hasFile(bufferId.toString()))
                && (hasDatabaseId = FileSystem
                        .hasFile(databaseId.toString()))) {
            UUID uuid = null;
            for (String file : files) {
                ByteBuffer bytes = FileSystem.readBytes(file);
                long mostSignificantBits = bytes.getLong();
                long leastSignificantBits = bytes.getLong();
                UUID stored = new UUID(mostSignificantBits,
                        leastSignificantBits);
                if(uuid == null || stored.equals(uuid)) {
                    uuid = stored;
                    continue;
                }
                else {
                    uuid = null;
                    break;
                }
            }
            return uuid;
        }
        else if(!hasBufferId && !hasDatabaseId) {
            UUID uuid = UUID.randomUUID();
            ByteBuffer bytes = ByteBuffer.allocate(16);
            bytes.putLong(uuid.getMostSignificantBits());
            bytes.putLong(uuid.getLeastSignificantBits());
            bytes.flip();
            for (String file : files) {
                FileSystem.writeBytes(ByteBuffers.asReadOnlyBuffer(bytes),
                        file);
            }
            return uuid;
        }
        return null;
    }

}
