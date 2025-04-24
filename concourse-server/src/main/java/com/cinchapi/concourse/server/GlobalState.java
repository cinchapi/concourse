/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import ch.qos.logback.classic.Level;

import com.cinchapi.common.base.Array;
import com.cinchapi.common.io.ByteBuffers;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.Constants;
import com.cinchapi.concourse.annotate.Experimental;
import com.cinchapi.concourse.annotate.NonPreference;
import com.cinchapi.concourse.config.ConcourseServerConfiguration;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.plugin.data.WriteEvent;
import com.cinchapi.concourse.server.storage.transporter.BatchTransporter;
import com.cinchapi.concourse.server.storage.transporter.StreamingTransporter;
import com.cinchapi.concourse.server.storage.transporter.Transporter;
import com.cinchapi.concourse.util.Networking;
import com.cinchapi.lib.config.read.Interpreters;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Contains configuration and state that must be accessible to various parts of
 * the Server.
 * 
 * @author Jeff Nelson
 */
public final class GlobalState extends Constants {

    /**
     * The absolute path to the root of the directory where Concourse Server is
     * installed. This value is set by the start script. When running from
     * Eclipse, this value is set to the launch directory.
     */
    @NonPreference
    public static String CONCOURSE_HOME = MoreObjects.firstNonNull(
            System.getProperty("com.cinchapi.concourse.server.home"),
            System.getProperty("user.dir"));

    // ========================================================================
    // =========================== SYSTEM METADATA ============================
    /**
     * A flag to indicate if the program is running from Eclipse. This flag has
     * a value of {@code true} if the JVM is launched with the
     * {@code -Declipse=true} flag.
     */
    private static final boolean RUNNING_FROM_ECLIPSE = System
            .getProperty("eclipse") != null
            && System.getProperty("eclipse").equals("true");

    // ========================================================================
    // ============================ PREFERENCES ================================
    /*
     * INSTRUCTIONS FOR ADDING CONFIGURATION PREFERENCES
     * 1. Create the appropriately named static variable and assigned it a
     * default value.
     * 2. Find the PREF READING BLOCK and attempt to read the value from the
     * prefs file, while supplying the variable you made in Step 1 as the
     * defaultValue.
     * 3. Add a placeholder for the new preference to the stock configuration
     * file within the "conf" directory.
     */

    /**
     * The path to the file that contains access credentials used to secure
     * access to {@link com.cinchapi.concourse.server.ConcourseServer Concourse
     * Server}.
     */
    public static String ACCESS_CREDENTIALS_FILE = ".access";

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
    public static final Class<?> REMOTE_INVOCATION_THREAD_CLASS = Reflection
            .getClassCasted(
                    "com.cinchapi.concourse.server.plugin.RemoteInvocationThread");

    /**
     * Whether log messages should also be printed to the console.
     */
    public static boolean ENABLE_CONSOLE_LOGGING = RUNNING_FROM_ECLIPSE ? true
            : false;

    /**
     * The length of the longest substring that will be indexed for fulltext
     * search.
     * <p>
     * Regardless of a word's length, this value controls the maximum length of
     * any substring of the word that is added to the search index. For optimal
     * performance, this value should be set to the longest possible substring
     * of a word that would be entered for a search operation. To be safe, it is
     * recommended to set this value to the length of the longest possible word
     * in the target language. For example, the longest possible word in English
     * is about 40 characters long.
     * </p>
     */
    public static int MAX_SEARCH_SUBSTRING_LENGTH = 40;

    /**
     * The password that is assigned to the root administrator account when
     * Concourse Server first starts.
     */
    public static String INIT_ROOT_PASSWORD = "admin";

    /**
     * The username that is assigned to the root administrator account when
     * Concourse Server first starts.
     */
    public static String INIT_ROOT_USERNAME = "admin";

    /**
     * Determines whether to use batch instead of streaming
     * {@link Transporter transports}.
     * <p>
     * When enabled, data is moved from the Buffer to the Database in the
     * background and in larger batches, which can improve overall throughput at
     * the cost of potentially longer pauses during merges.
     */
    public static boolean ENABLE_BATCH_TRANSPORTS = false;

    /**
     * The type of {@link Transporter} to use when transporting data from the
     * Buffer to the Database.
     */
    @NonPreference
    public static Class<? extends Transporter> TRANSPORTER_CLASS = StreamingTransporter.class;

    /**
     * The number of threads to use for {@link Transporter#transport()
     * transport} operations. More threads can improve transport throughput in
     * some scenarios, but may increase resource contention.
     */
    public static int NUM_TRANSPORTER_THREADS = 1;

    /**
     * Potentially use multiple threads to asynchronously read data from disk.
     * <p>
     * When enabled, reads will typically be faster when accessing data too
     * large to fit in memory or no longer cached due to memory constraints.
     * </p>
     * <p>
     * This setting is particularly useful for search data since those indexes
     * are not cached by default (unless {@link #ENABLE_SEARCH_CACHE} is
     * enabled). Even if search records are cached, this setting may still
     * provide a performance boost if the size of some search metadata exceeds
     * the limits of what is cacheable in memory.
     * </p>
     * <p>
     * <strong>NOTE:</strong> There might be some overhead that could make some
     * reads slower if all their relevant segment metadata is cached and there
     * is high contention.
     * </p>
     */
    @Experimental
    public static boolean ENABLE_ASYNC_DATA_READS = false;

    /**
     * Automatically use a combination of defragmentation, garbage collection
     * and load balancing within the data files to optimize storage for read
     * performance.
     * <p>
     * The compaction process runs continuously in the background without
     * disrupting reads or writes. The storage engine uses a specific strategy
     * to determine how data files should be reorganized to improve the
     * performance of read operations.
     * </p>
     */
    @Experimental
    public static boolean ENABLE_COMPACTION = false;

    /**
     * Maintain and in-memory cache of the data indexes used to respond to
     * {@link com.cinchapi.concourse.Concourse#search(String, String)} commands.
     * <p>
     * Search indexes tend to be much larger than those used for primary and
     * secondary lookups, so enabling the search cache may cause
     * memory issues (and overall performance degradation) if search is heavily
     * used. Furthermore, indexing and write performance may also suffer if
     * cached search indexes must be incrementally kept current.
     * </p>
     */
    @Experimental
    public static boolean ENABLE_SEARCH_CACHE = false;

    /**
     * Attempt to optimize
     * {@link com.cinchapi.concourse.Concourse#verify(String, Object, long)
     * verify} commands by using special lookup records.
     * <p>
     * The database does not cache lookup records, so, while generating one is
     * theoretically faster than generating a full or partial record, repeated
     * attempts to verify data in the same field (e.g. a counter whose value is
     * stored against a single locator/key) or record may be slower due to lack
     * of caching.
     * </p>
     */
    @Experimental
    public static boolean ENABLE_VERIFY_BY_LOOKUP = false;

    /**
     * Use a more memory-efficient representation for storage metadata.
     * <p>
     * On average, enabling this setting will reduce the amount of heap space
     * needed for essential metadata by 33%. As a result, overall system
     * performance may improve due to a reduction in garbage collection pauses.
     * </p>
     * <p>
     * However, this setting may increase CPU usage and slightly reduce
     * peak performance on a per-operation basis due to weaker reference
     * locality.
     * </p>
     */
    @Experimental
    public static boolean ENABLE_EFFICIENT_METADATA = false;

    static {
        List<String> files = ImmutableList.of(
                "conf" + File.separator + "concourse.prefs",
                "conf" + File.separator + "concourse.yaml",
                "conf" + File.separator + "concourse.prefs.dev",
                "conf" + File.separator + "concourse.yaml.dev");
        ConcourseServerConfiguration config = ConcourseServerConfiguration
                .from(files.stream()
                        .map(file -> Paths.get(FileSystem.expandPath(file)))
                        .collect(Collectors.toList())
                        .toArray(Array.containing()));

        // @formatter:off
        Map<String, Class<? extends Transporter>> transporterClasses = ImmutableMap
                .<String, Class<? extends Transporter>> builder()
                .put("streaming", StreamingTransporter.class)
                .put(StreamingTransporter.class.getName(), StreamingTransporter.class)
                .put(StreamingTransporter.class.getSimpleName(), StreamingTransporter.class)
                .put("batch", BatchTransporter.class)
                .put(BatchTransporter.class.getName(), BatchTransporter.class)
                .put(BatchTransporter.class.getSimpleName(), BatchTransporter.class)
                .build();
        // @formatter:on

        // =================== PREF READING BLOCK ====================
        ACCESS_CREDENTIALS_FILE = FileSystem
                .expandPath(config.getOrDefault("access_credentials_file",
                        ACCESS_CREDENTIALS_FILE), CONCOURSE_HOME);

        DATABASE_DIRECTORY = config.getOrDefault("database_directory",
                DATABASE_DIRECTORY);

        BUFFER_DIRECTORY = config.getOrDefault("buffer_directory",
                BUFFER_DIRECTORY);

        BUFFER_PAGE_SIZE = (int) config.getSize("buffer_page_size",
                BUFFER_PAGE_SIZE);

        CLIENT_PORT = config.getOrDefault("client_port", CLIENT_PORT);

        SHUTDOWN_PORT = config.getOrDefault("shutdown_port",
                Networking.getCompanionPort(CLIENT_PORT, 2));

        JMX_PORT = config.getOrDefault("jmx_port", JMX_PORT);

        HEAP_SIZE = config.getSize("heap_size", HEAP_SIZE);

        HTTP_PORT = config.getOrDefault("http_port", HTTP_PORT);

        HTTP_ENABLE_CORS = config.getOrDefault("http_enable_cors",
                HTTP_ENABLE_CORS);

        HTTP_CORS_DEFAULT_ALLOW_ORIGIN = config.getOrDefault(
                "http_cors_default_allow_origin",
                HTTP_CORS_DEFAULT_ALLOW_ORIGIN);

        HTTP_CORS_DEFAULT_ALLOW_HEADERS = config.getOrDefault(
                "http_cors_default_allow_headers",
                HTTP_CORS_DEFAULT_ALLOW_HEADERS);

        HTTP_CORS_DEFAULT_ALLOW_METHODS = config.getOrDefault(
                "http_cors_default_allow_methods",
                HTTP_CORS_DEFAULT_ALLOW_METHODS);

        LOG_LEVEL = config.getOrDefault("log_level", Interpreters.logLevel(),
                LOG_LEVEL);

        ENABLE_CONSOLE_LOGGING = config.getOrDefault("enable_console_logging",
                ENABLE_CONSOLE_LOGGING);
        if(!ENABLE_CONSOLE_LOGGING) {
            ENABLE_CONSOLE_LOGGING = Boolean.parseBoolean(System.getProperty(
                    "com.cinchapi.concourse.server.logging.console", "false"));
        }
        DEFAULT_ENVIRONMENT = config.getOrDefault("default_environment",
                DEFAULT_ENVIRONMENT);

        MANAGEMENT_PORT = config.getOrDefault("management_port",
                Networking.getCompanionPort(CLIENT_PORT, 4));

        SYSTEM_ID = getSystemId();

        MAX_SEARCH_SUBSTRING_LENGTH = config.getOrDefault(
                "max_search_substring_length", MAX_SEARCH_SUBSTRING_LENGTH);

        ENABLE_ASYNC_DATA_READS = config.getOrDefault("enable_async_data_reads",
                ENABLE_ASYNC_DATA_READS);

        ENABLE_COMPACTION = config.getOrDefault("enable_compaction",
                ENABLE_COMPACTION);

        ENABLE_SEARCH_CACHE = config.getOrDefault("enable_search_cache",
                ENABLE_SEARCH_CACHE);

        ENABLE_VERIFY_BY_LOOKUP = config.getOrDefault("enable_verify_by_lookup",
                ENABLE_VERIFY_BY_LOOKUP);

        INIT_ROOT_PASSWORD = config.getOrDefault("init.root.password",
                config.getOrDefault("init_root_password", INIT_ROOT_PASSWORD));

        INIT_ROOT_USERNAME = config.getOrDefault("init.root.username",
                config.getOrDefault("init_root_username", INIT_ROOT_USERNAME));

        ENABLE_EFFICIENT_METADATA = config.getOrDefault(
                "enable_efficient_metadata", ENABLE_EFFICIENT_METADATA);

        Object transporter = config.get("transporter");
        String transporterType;
        if(transporter != null && transporter instanceof Map) {
            transporterType = config.getOrDefault("transporter.type",
                    transporter.toString());
            NUM_TRANSPORTER_THREADS = config.getOrDefault(
                    "transporter.num_threads", NUM_TRANSPORTER_THREADS);
        }
        else {
            transporterType = transporter != null ? transporter.toString() : "";
        }
        TRANSPORTER_CLASS = transporterClasses.getOrDefault(transporterType,
                TRANSPORTER_CLASS);
        ENABLE_BATCH_TRANSPORTS = TRANSPORTER_CLASS == BatchTransporter.class;

        // =================== PREF READING BLOCK ====================
    }

    /**
     * A global {@link BlockingQueue} that is populated with {@link WriteEvent
     * write events} within each environment's {@link Buffer}.
     */
    @NonPreference
    public static LinkedBlockingQueue<WriteEvent> BINARY_QUEUE = new LinkedBlockingQueue<WriteEvent>();

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
     * The number of bytes to read from disk at a time.
     */
    @NonPreference
    public static final int DISK_READ_BUFFER_SIZE = (int) Math.pow(2, 20); // 1048567
                                                                           // (~1MiB)

    /**
     * The maximum number of bytes to reserve for caching data. This cache
     * memory quota is shared by multiple caches per environment and must be
     * lower than the overall {@link #HEAP_SIZE}. It represents the approximate
     * total space that can be used for caching non-essential storage metadata
     * in the entire system.
     */
    @NonPreference
    public static int CACHE_MEMORY_LIMIT = (int) (.5 * HEAP_SIZE);

    /**
     * The frequency, in seconds, at which the memory usage of each cached value
     * is checked. This periodic check is essential to ensuring that size-based
     * eviction is accurately enforced as cached values are incrementally
     * updated with new data writes.
     */
    @NonPreference
    public static int CACHE_MEMORY_CHECK_FREQUENCY = 60;

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
        List<String> files = ImmutableList.of(bufferId.toString(),
                databaseId.toString());
        boolean hasBufferId = FileSystem.hasFile(bufferId);
        boolean hasDatabaseId = FileSystem.hasFile(databaseId);
        if(hasBufferId && hasDatabaseId) { // Verify that the system id in the
                                           // database and buffer are
                                           // consistent.
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
        else if(!hasBufferId && !hasDatabaseId) { // Create a system id and
                                                  // store it in the database
                                                  // and buffer.
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
        else { // The system id is not consistent across the buffer and
               // database.
            return null;
        }
    }

}
