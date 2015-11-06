/*
 * Copyright (c) 2015 Cinchapi, Inc.
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
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.SocketException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import jline.TerminalFactory;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.DuplicateEntryException;
import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.config.ConcourseServerPreferences;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.Symbol;
import com.cinchapi.concourse.thrift.Diff;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.time.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;

import com.cinchapi.concourse.util.ConcourseServerDownloader;
import com.cinchapi.concourse.util.Processes;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * A {@link ManagedConcourseServer} is an external server process that can be
 * programmatically controlled within another application. This class is useful
 * for applications that want to "embed" a Concourse Server for the duration of
 * the application's life cycle and then forget about its existence afterwards.
 * 
 * @author jnelson
 */
public class ManagedConcourseServer {

    /**
     * Return an {@link ManagedConcourseServer} that controls an instance
     * located in the {@code installDirectory}.
     * 
     * @param installDirectory
     * @return the ManagedConcourseServer
     */
    public static ManagedConcourseServer manageExistingServer(
            String installDirectory) {
        return new ManagedConcourseServer(installDirectory);
    }

    /**
     * Create an {@link ManagedConcourseServer from the {@code installer}.
     * 
     * @param installer
     * @return the ManagedConcourseServer
     */
    public static ManagedConcourseServer manageNewServer(File installer) {
        return manageNewServer(installer, DEFAULT_INSTALL_HOME + File.separator
                + Time.now());
    }

    /**
     * Create an {@link ManagedConcourseServer} from the {@code installer} in
     * {@code directory}.
     * 
     * @param installer
     * @param directory
     * @return the EmbeddedConcourseServer
     */
    public static ManagedConcourseServer manageNewServer(File installer,
            String directory) {
        return new ManagedConcourseServer(install(installer.getAbsolutePath(),
                directory));
    }

    /**
     * Create an {@link ManagedConcourseServer} at {@code version}.
     * 
     * @param version
     * @return the ManagedConcourseServer
     */
    public static ManagedConcourseServer manageNewServer(String version) {
        return manageNewServer(version, DEFAULT_INSTALL_HOME + File.separator
                + Time.now());
    }

    /**
     * Create an {@link ManagedConcourseServer} at {@code version} in
     * {@code directory}.
     * 
     * @param version
     * @param directory
     * @return the ManagedConcourseServer
     */
    public static ManagedConcourseServer manageNewServer(String version,
            String directory) {
        return manageNewServer(
                new File(ConcourseServerDownloader.download(version)),
                directory);
    }

    /**
     * Tweak some of the preferences to make this more palatable for testing
     * (i.e. reduce the possibility of port conflicts, etc).
     * 
     * @param installDirectory
     */
    private static void configure(String installDirectory) {
        ConcourseServerPreferences prefs = ConcourseServerPreferences
                .open(installDirectory + File.separator + CONF + File.separator
                        + "concourse.prefs");
        String data = installDirectory + File.separator + "data";
        prefs.setBufferDirectory(data + File.separator + "buffer");
        prefs.setDatabaseDirectory(data + File.separator + "database");
        prefs.setClientPort(getOpenPort());
        prefs.setJmxPort(getOpenPort());
        prefs.setLogLevel(Level.DEBUG);
        prefs.setShutdownPort(getOpenPort());
    }

    /**
     * Collect and return all the {@code jar} files that are located in the
     * directory at {@code path}. If {@code path} is not a directory, but is
     * instead, itself, a jar file, then return a list that contains in.
     * 
     * @param path
     * @return the list of jar file URL paths
     */
    private static URL[] gatherJars(String path) {
        List<URL> jars = Lists.newArrayList();
        gatherJars(path, jars);
        return jars.toArray(new URL[] {});
    }

    /**
     * Collect all the {@code jar} files that are located in the directory at
     * {@code path} and place them into the list of {@code jars}. If
     * {@code path} is not a directory, but is instead, itself a jar file, then
     * place it in the list.
     * 
     * @param path
     * @param jars
     */
    private static void gatherJars(String path, List<URL> jars) {
        try {
            if(Files.isDirectory(Paths.get(path))) {
                for (Path p : Files.newDirectoryStream(Paths.get(path))) {
                    gatherJars(p.toString(), jars);
                }
            }
            else if(path.endsWith(".jar")) {
                jars.add(new URL("file://" + path.toString()));
            }
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Get an open port.
     * 
     * @return the port
     */
    private static int getOpenPort() {
        int min = 49512;
        int max = 65535;
        int port = RAND.nextInt(min) + (max - min);
        return isPortAvailable(port) ? port : getOpenPort();
    }

    /**
     * Install a Concourse Server in {@code directory} using {@code installer}.
     * 
     * @param installer
     * @param directory
     * @return the server install directory
     */
    private static String install(String installer, String directory) {
        try {
            Files.createDirectories(Paths.get(directory));
            Path binary = Paths.get(directory + File.separator
                    + TARGET_BINARY_NAME);
            Files.deleteIfExists(binary);
            Files.copy(Paths.get(installer), binary);
            ProcessBuilder builder = new ProcessBuilder(Lists.newArrayList(
                    "sh", binary.toString()));
            builder.directory(new File(directory));
            builder.redirectErrorStream();
            Process process = builder.start();
            // The concourse-server installer prompts for an admin password in
            // order to make optional system wide In order to get around this
            // prompt, we have to "kill" the process, otherwise the server
            // install will hang.
            Stopwatch watch = Stopwatch.createStarted();
            while (watch.elapsed(TimeUnit.SECONDS) < 1) {
                continue;
            }
            watch.stop();
            process.destroy();
            TerminalFactory.get().restore();
            String application = directory + File.separator
                    + "concourse-server"; // the install directory for the
                                          // concourse-server application
            process = Runtime.getRuntime().exec("ls " + application);
            List<String> output = Processes.getStdOut(process);
            if(!output.isEmpty()) {
                configure(application);
                log.info("Successfully installed server in {}", application);
                return application;
            }
            else {
                throw new RuntimeException(
                        MessageFormat
                                .format("Unsuccesful attempt to "
                                        + "install server at {0} "
                                        + "using binary from {1}", directory,
                                        installer));
            }

        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }

    }

    /**
     * Return {@code true} if the {@code port} is available on the local
     * machine.
     * 
     * @param port
     * @return {@code true} if the port is available
     */
    private static boolean isPortAvailable(int port) {
        try {
            new ServerSocket(port).close();
            return true;
        }
        catch (SocketException e) {
            return false;
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private static final String BIN = "bin";

    // ---relative paths
    private static final String CONF = "conf";

    /**
     * The default location where the the test server is installed if a
     * particular location is not specified.
     */
    private static final String DEFAULT_INSTALL_HOME = System
            .getProperty("user.home") + File.separator + ".concourse-testing";
    // ---logger
    private static final Logger log = LoggerFactory
            .getLogger(ManagedConcourseServer.class);

    // ---random
    private static final Random RAND = new Random();

    /**
     * The filename of the binary installer from which the test server will be
     * created.
     */
    private static final String TARGET_BINARY_NAME = "concourse-server.bin";

    /**
     * The server application install directory;
     */
    private final String installDirectory;

    /**
     * A connection to the remote MBean server running in the managed
     * concourse-server process.
     */
    private MBeanServerConnection mBeanServerConnection = null;

    /**
     * The handler for the server's preferences.
     */
    private final ConcourseServerPreferences prefs;

    /**
     * Construct a new instance.
     * 
     * @param installDirectory
     */
    private ManagedConcourseServer(String installDirectory) {
        this.installDirectory = installDirectory;
        this.prefs = ConcourseServerPreferences.open(installDirectory
                + File.separator + CONF + File.separator + "concourse.prefs");
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

            @Override
            public void run() {
                destroy();
            }

        }));
    }

    /**
     * Return a connection handler to the server using the default "admin"
     * credentials.
     * 
     * @return the connection handler
     */
    public Concourse connect() {
        return connect("admin", "admin");
    }

    /**
     * Return a connection handler to the server using the specified
     * {@code username} and {@code password}.
     * 
     * @param username
     * @param password
     * @return the connection handler
     */
    public Concourse connect(String username, String password) {
        return new Client(username, password);
    }

    /**
     * Stop the server, if it is running, and permanently delete the application
     * files and associated data.
     */
    public void destroy() {
        if(Files.exists(Paths.get(installDirectory))) { // check if server has
                                                        // been manually
                                                        // destroyed
            if(isRunning()) {
                stop();
            }
            try {
                deleteDirectory(Paths.get(installDirectory).getParent()
                        .toString());
                log.info("Deleted server install directory at {}",
                        installDirectory);
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }

    }

    /**
     * Return the client port for this server.
     * 
     * @return the client port
     */
    public int getClientPort() {
        return prefs.getClientPort();
    }

    /**
     * Get a collection of stats about the heap memory usage for the managed
     * concourse-server process.
     * 
     * @return the heap memory usage
     */
    public MemoryUsage getHeapMemoryStats() {
        try {
            MemoryMXBean memory = ManagementFactory.newPlatformMXBeanProxy(
                    getMBeanServerConnection(),
                    ManagementFactory.MEMORY_MXBEAN_NAME, MemoryMXBean.class);
            return memory.getHeapMemoryUsage();
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Return the {@link #installDirectory} for this server.
     * 
     * @return the install directory
     */
    public String getInstallDirectory() {
        return installDirectory;
    }

    /**
     * Return the connection to the MBean sever of the managed concourse-server
     * process.
     * 
     * @return the mbean server connection
     */
    public MBeanServerConnection getMBeanServerConnection() {
        if(mBeanServerConnection == null) {
            try {
                JMXServiceURL url = new JMXServiceURL(
                        "service:jmx:rmi:///jndi/rmi://localhost:"
                                + prefs.getJmxPort() + "/jmxrmi");
                JMXConnector connector = JMXConnectorFactory.connect(url);
                mBeanServerConnection = connector.getMBeanServerConnection();
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
        return mBeanServerConnection;
    }

    /**
     * Get a collection of stats about the non heap memory usage for the managed
     * concourse-server process.
     * 
     * @return the non-heap memory usage
     */
    public MemoryUsage getNonHeapMemoryStats() {
        try {
            MemoryMXBean memory = ManagementFactory.newPlatformMXBeanProxy(
                    getMBeanServerConnection(),
                    ManagementFactory.MEMORY_MXBEAN_NAME, MemoryMXBean.class);
            return memory.getNonHeapMemoryUsage();
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Return {@code true} if the server is currently running.
     * 
     * @return {@code true} if the server is running
     */
    public boolean isRunning() {
        return Iterables.get(execute("concourse", "status"), 0).contains(
                "is running");
    }

    /**
     * Start the server.
     */
    public void start() {
        try {
            for (String line : execute("start")) {
                log.info(line);
            }
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }

    }

    /**
     * Stop the server.
     */
    public void stop() {
        try {
            for (String line : execute("stop")) {
                log.info(line);
            }
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }

    }

    /**
     * Recursively delete a directory and all of its contents.
     * 
     * @param directory
     */
    private void deleteDirectory(String directory) {
        try {
            File dir = new File(directory);
            for (File file : dir.listFiles()) {
                if(file.isDirectory()) {
                    deleteDirectory(file.getAbsolutePath());
                }
                else {
                    file.delete();
                }
            }
            dir.delete();
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Execute a command line interface script and return the output.
     * 
     * @param cli
     * @param args
     * @return the script output
     */
    private List<String> execute(String cli, String... args) {
        try {
            String command = "sh " + cli;
            for (String arg : args) {
                command += " " + arg;
            }
            Process process = Runtime.getRuntime().exec(command, null,
                    new File(installDirectory + File.separator + BIN));
            return Processes.getStdOut(process);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * A class that extends {@link Concourse} with additional methods. This
     * abstraction allows casting while keeping the {@link Client} class
     * private.
     * 
     * @author Jeff Nelson
     */
    public abstract class ReflectiveClient extends Concourse {

        /**
         * Reflectively call a client method. This is useful for cases where
         * this API depends on an older version of the Concourse API and a test
         * needs to use a method added in a later version. This procedure will
         * work because the underlying client delegates to a client that uses
         * the classpath of the managed server.
         * 
         * @param methodName
         * @param args
         * @return the result
         */
        public abstract <T> T call(String methodName, Object... args);
    }

    /**
     * A {@link Concourse} client wrapper that delegates to the jars located in
     * the server's lib directory so that it uses the same version of the code.
     * 
     * @author jnelson
     */
    private final class Client extends ReflectiveClient {

        private Class<?> clazz;
        private final Object delegate;
        private ClassLoader loader;

        /**
         * The top level package under which all Concourse classes exist in the
         * remote server.
         */
        private String packageBase = "com.cinchapi.concourse.";

        /**
         * Construct a new instance.
         * 
         * @param username
         * @param password
         * @throws Exception
         */
        public Client(String username, String password) {
            try {
                this.loader = new URLClassLoader(
                        gatherJars(getInstallDirectory()), null);
                try {
                    clazz = loader.loadClass(packageBase + "Concourse");
                }
                catch (ClassNotFoundException e) {
                    // Prior to version 0.5.0, Concourse classes were located in
                    // the "org.cinchapi.concourse" package, so we attempt to
                    // use that if the default does not work.
                    packageBase = "org.cinchapi.concourse.";
                    clazz = loader.loadClass(packageBase + "Concourse");
                }
                this.delegate = clazz.getMethod("connect", String.class,
                        int.class, String.class, String.class).invoke(null,
                        "localhost", getClientPort(), username, password);
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public void abort() {
            invoke("abort").with();
        }

        @Override
        public <T> Map<Long, Boolean> add(String key, T value,
                Collection<Long> records) {
            return invoke("add", String.class, Object.class, Collection.class)
                    .with(key, value, records);
        }

        @Override
        public <T> long add(String key, T value) {
            return invoke("add", String.class, Object.class).with(key, value);
        }

        @Override
        public <T> boolean add(String key, T value, long record) {
            return invoke("add", String.class, Object.class, long.class).with(
                    key, value, record);
        }

        @Override
        public Map<Timestamp, String> audit(long record) {
            return invoke("audit", long.class).with(record);
        }

        @Override
        public Map<Timestamp, String> audit(long record, Timestamp start) {
            return invoke("audit", long.class, Timestamp.class).with(record,
                    start);
        }

        @Override
        public Map<Timestamp, String> audit(long record, Timestamp start,
                Timestamp end) {
            return invoke("audit", long.class, Timestamp.class, Timestamp.class)
                    .with(start, end);
        }

        @Override
        public Map<Timestamp, String> audit(String key, long record) {
            return invoke("audit", String.class, long.class).with(key, record);
        }

        @Override
        public Map<Timestamp, String> audit(String key, long record,
                Timestamp start) {
            return invoke("audit", String.class, long.class, Timestamp.class)
                    .with(key, record, start);
        }

        @Override
        public Map<Timestamp, String> audit(String key, long record,
                Timestamp start, Timestamp end) {
            return invoke("audit", String.class, long.class, Timestamp.class,
                    Timestamp.class).with(key, record, start, end);
        }

        @Override
        public Map<String, Map<Object, Set<Long>>> browse(
                Collection<String> keys) {
            return invoke("browse", Collection.class, Object.class).with(keys);
        }

        @Override
        public Map<String, Map<Object, Set<Long>>> browse(
                Collection<String> keys, Timestamp timestamp) {
            return invoke("browse", Collection.class, Timestamp.class).with(
                    keys, timestamp);
        }

        @Override
        public Map<Object, Set<Long>> browse(String key) {
            return invoke("browse", String.class).with(key);
        }

        @Override
        public Map<Object, Set<Long>> browse(String key, Timestamp timestamp) {
            return invoke("browse", String.class, Timestamp.class).with(key,
                    timestamp);
        }

        @Override
        public <T> T call(String methodName, Object... args) {
            Class<?>[] classes = new Class<?>[args.length];
            for (int i = 0; i < classes.length; ++i) {
                classes[i] = args[i].getClass();
            }
            return invoke(methodName, classes).with(args);
        }

        @Override
        public Map<Timestamp, Set<Object>> chronologize(String key, long record) {
            return invoke("chronologize", String.class, long.class).with(key,
                    record);
        }

        @Override
        public Map<Timestamp, Set<Object>> chronologize(String key,
                long record, Timestamp start) {
            return invoke("chronologize", String.class, long.class,
                    Timestamp.class).with(key, record, start);
        }

        @Override
        public Map<Timestamp, Set<Object>> chronologize(String key,
                long record, Timestamp start, Timestamp end) {
            return invoke("chronologize", String.class, long.class,
                    Timestamp.class, Timestamp.class).with(key, record, start,
                    end);
        }

        @Override
        public void clear(Collection<Long> records) {
            invoke("clear", Collection.class).with(records);
        }

        @Override
        public void clear(Collection<String> keys, Collection<Long> records) {
            invoke("clear", Collection.class, Collection.class).with(keys,
                    records);
        }

        @Override
        public void clear(Collection<String> keys, long record) {
            invoke("clear", Collection.class, long.class).with(keys, record);
        }

        @Override
        public void clear(long record) {
            invoke("clear", long.class).with(record);
        }

        @Override
        public void clear(String key, Collection<Long> records) {
            invoke("clear", String.class, Collection.class).with(key, records);

        }

        @Override
        public void clear(String key, long record) {
            invoke("clear", String.class, long.class).with(key, record);

        }

        @Override
        public boolean commit() {
            return invoke("commit").with();
        }

        @Override
        public long create() {
            return invoke("create").with();
        }

        @Override
        public Map<Long, Set<String>> describe(Collection<Long> records) {
            return invoke("describe", Collection.class).with(records);
        }

        @Override
        public Map<Long, Set<String>> describe(Collection<Long> records,
                Timestamp timestamp) {
            return invoke("describe", Collection.class, Timestamp.class).with(
                    records, timestamp);
        }

        @Override
        public Set<String> describe(long record) {
            return invoke("describe", long.class).with(record);
        }

        @Override
        public Set<String> describe(long record, Timestamp timestamp) {
            return invoke("describe", long.class, Timestamp.class).with(record,
                    timestamp);
        }

        @Override
        public <T> Map<String, Map<Diff, Set<T>>> diff(long record,
                Timestamp start) {
            return invoke("diff", long.class, Timestamp.class).with(record,
                    start);
        }

        @Override
        public <T> Map<String, Map<Diff, Set<T>>> diff(long record,
                Timestamp start, Timestamp end) {
            return invoke("diff", long.class, Timestamp.class, Timestamp.class)
                    .with(record, start, end);
        }

        @Override
        public <T> Map<Diff, Set<T>> diff(String key, long record,
                Timestamp start) {
            return invoke("diff", String.class, long.class, Timestamp.class)
                    .with(record, start);
        }

        @Override
        public <T> Map<Diff, Set<T>> diff(String key, long record,
                Timestamp start, Timestamp end) {
            return invoke("diff", String.class, long.class, Timestamp.class,
                    Timestamp.class).with(key, record, start, end);
        }

        @Override
        public <T> Map<T, Map<Diff, Set<Long>>> diff(String key, Timestamp start) {
            return invoke("diff", String.class, Timestamp.class).with(key,
                    start);
        }

        @Override
        public <T> Map<T, Map<Diff, Set<Long>>> diff(String key,
                Timestamp start, Timestamp end) {
            return invoke("diff", String.class, Timestamp.class).with(key,
                    start);
        }

        @Override
        public void exit() {
            invoke("exit").with();
        }

        @Override
        public Set<Long> find(Criteria criteria) {
            return invoke("find", Criteria.class).with(criteria);
        }

        @Override
        public Set<Long> find(Object criteria) {
            return invoke("find", Object.class).with(criteria);
        }

        @Override
        public Set<Long> find(String ccl) {
            return invoke("find", String.class, Object.class).with(ccl);
        }

        @Override
        public Set<Long> find(String key, Object value) {
            return invoke("find", String.class, Object.class).with(key, value);
        }

        @Override
        public Set<Long> find(String key, Operator operator, Object value) {
            return invoke("find", String.class, Operator.class, Object.class)
                    .with(key, operator, value);
        }

        @Override
        public Set<Long> find(String key, Operator operator, Object value,
                Object value2) {
            return invoke("find", String.class, Operator.class, Object.class,
                    Object.class).with(key, operator, value, value2);
        }

        @Override
        public Set<Long> find(String key, Operator operator, Object value,
                Object value2, Timestamp timestamp) {
            return invoke("find", String.class, Operator.class, Object.class,
                    Object.class, Timestamp.class).with(key, operator, value,
                    value2);
        }

        @Override
        public Set<Long> find(String key, Operator operator, Object value,
                Timestamp timestamp) {
            return invoke("find", String.class, Operator.class, Object.class,
                    Timestamp.class).with(key, operator, value, timestamp);
        }

        @Override
        public Set<Long> find(String key, String operator, Object value) {
            return invoke("find", String.class, String.class, Object.class)
                    .with(key, operator, value);
        }

        @Override
        public Set<Long> find(String key, String operator, Object value,
                Object value2) {
            return invoke("find", String.class, String.class, Object.class,
                    Object.class).with(key, operator, value, value2);
        }

        @Override
        public Set<Long> find(String key, String operator, Object value,
                Object value2, Timestamp timestamp) {
            return invoke("find", String.class, String.class, Object.class,
                    Object.class, Timestamp.class).with(key, operator, value,
                    value2, timestamp);
        }

        @Override
        public Set<Long> find(String key, String operator, Object value,
                Timestamp timestamp) {
            return invoke("find", String.class, String.class, Object.class,
                    Timestamp.class).with(key, operator, value, timestamp);
        }

        @Override
        public <T> long findOrAdd(String key, T value)
                throws DuplicateEntryException {
            return invoke("findOrAdd", String.class, Object.class).with(key,
                    value);
        }

        @Override
        public long findOrInsert(Criteria criteria, String json)
                throws DuplicateEntryException {
            return invoke("findOrInsert", Criteria.class, String.class).with(
                    criteria, json);
        }

        @Override
        public long findOrInsert(String ccl, String json)
                throws DuplicateEntryException {
            return invoke("findOrInsert", String.class, String.class).with(ccl,
                    json);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
                Collection<Long> records) {
            return invoke("get", Collection.class, Collection.class).with(keys,
                    records);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
                Collection<Long> records, Timestamp timestamp) {
            return invoke("get", Collection.class, Collection.class,
                    Timestamp.class).with(keys, records, timestamp);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
                Criteria criteria) {
            return invoke("get", Collection.class, Criteria.class).with(keys,
                    criteria);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
                Criteria criteria, Timestamp timestamp) {
            return invoke("get", Collection.class, Criteria.class,
                    Timestamp.class).with(keys, criteria, timestamp);
        }

        @Override
        public <T> Map<String, T> get(Collection<String> keys, long record) {
            return invoke("get", String.class, long.class).with(keys, record);
        }

        @Override
        public <T> Map<String, T> get(Collection<String> keys, long record,
                Timestamp timestamp) {
            return invoke("get", Collection.class, long.class, Timestamp.class)
                    .with(keys, record, timestamp);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
                Object criteria) {
            return invoke("get", Collection.class, Object.class).with(keys,
                    criteria);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
                Object criteria, Timestamp timestamp) {
            return invoke("get", Collection.class, Object.class,
                    Timestamp.class).with(keys, criteria, timestamp);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
                String ccl) {
            return invoke("get", Collection.class, String.class)
                    .with(keys, ccl);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
                String ccl, Timestamp timestamp) {
            return invoke("get", Collection.class, String.class,
                    Timestamp.class).with(keys, ccl, timestamp);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(Criteria criteria) {
            return invoke("get", Criteria.class).with(criteria);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(Criteria criteria,
                Timestamp timestamp) {
            return invoke("get", Criteria.class, Timestamp.class).with(
                    criteria, timestamp);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(Object criteria) {
            return invoke("get", Object.class).with(criteria);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(Object criteria,
                Timestamp timestamp) {
            return invoke("get", Object.class, Timestamp.class).with(criteria,
                    timestamp);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(String ccl) {
            return invoke("get", String.class).with(ccl);
        }

        @Override
        public <T> Map<Long, T> get(String key, Collection<Long> records) {
            return invoke("get", String.class, Collection.class).with(key,
                    records);
        }

        @Override
        public <T> Map<Long, T> get(String key, Collection<Long> records,
                Timestamp timestamp) {
            return invoke("get", String.class, Collection.class,
                    Timestamp.class).with(key, records, timestamp);
        }

        @Override
        public <T> Map<Long, T> get(String key, Criteria criteria) {
            return invoke("get", String.class, Criteria.class).with(key,
                    criteria);
        }

        @Override
        public <T> Map<Long, T> get(String key, Criteria criteria,
                Timestamp timestamp) {
            return invoke("get", String.class, Criteria.class, Timestamp.class)
                    .with(key, criteria, timestamp);
        }

        @Override
        public <T> T get(String key, long record) {
            return invoke("get", String.class, long.class).with(key, record);
        }

        @Override
        public <T> T get(String key, long record, Timestamp timestamp) {
            return invoke("get", String.class, long.class, Timestamp.class)
                    .with(key, record, timestamp);
        }

        @Override
        public <T> Map<Long, T> get(String key, Object criteria) {
            return invoke("find", String.class, Object.class).with(key,
                    criteria);
        }

        @Override
        public <T> Map<Long, T> get(String key, Object criteria,
                Timestamp timestamp) {
            return invoke("get", String.class, Object.class, Timestamp.class)
                    .with(key, criteria, timestamp);
        }

        @Override
        public <T> Map<Long, T> get(String key, String ccl) {
            return invoke("find", String.class, String.class).with(key, ccl);
        }

        @Override
        public <T> Map<Long, T> get(String key, String ccl, Timestamp timestamp) {
            return invoke("find", String.class, String.class, Timestamp.class)
                    .with(key, ccl, timestamp);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(String ccl, Timestamp timestamp) {
            return invoke("get", String.class, Timestamp.class).with(ccl,
                    timestamp);
        }

        @Override
        public String getServerEnvironment() {
            return invoke("getServerEnvironment").with();
        }

        @Override
        public String getServerVersion() {
            return invoke("getServerVersion").with();
        }

        @Override
        public Set<Long> insert(String json) {
            return invoke("insert", String.class).with(json);
        }

        @Override
        public Map<Long, Boolean> insert(String json, Collection<Long> records) {
            return invoke("insert", String.class, Collection.class).with(json,
                    records);
        }

        @Override
        public boolean insert(String json, long record) {
            return invoke("insert", String.class, long.class)
                    .with(json, record);
        }

        @Override
        public Set<Long> inventory() {
            return invoke("inventory").with();
        }

        @Override
        public String jsonify(Collection<Long> records) {
            return invoke("jsonify", Collection.class).with(records);
        }

        @Override
        public String jsonify(Collection<Long> records, boolean identifier) {
            return invoke("jsonify", Collection.class, boolean.class).with(
                    records, identifier);
        }

        @Override
        public String jsonify(Collection<Long> records, Timestamp timestamp) {
            return invoke("jsonify", Collection.class, Timestamp.class).with(
                    records, timestamp);
        }

        @Override
        public String jsonify(Collection<Long> records, Timestamp timestamp,
                boolean identifier) {
            return invoke("jsonify", Collection.class, Timestamp.class,
                    boolean.class).with(records, timestamp, identifier);
        }

        @Override
        public String jsonify(long record) {
            return invoke("jsonify", long.class).with(record);
        }

        @Override
        public String jsonify(long record, boolean identifier) {
            return invoke("jsonify", long.class, boolean.class).with(record,
                    identifier);
        }

        @Override
        public String jsonify(long record, Timestamp timestamp) {
            return invoke("jsonify", long.class, Timestamp.class).with(record,
                    timestamp);
        }

        @Override
        public String jsonify(long record, Timestamp timestamp,
                boolean identifier) {
            return invoke("jsonify", long.class, Timestamp.class, boolean.class)
                    .with(record, timestamp, identifier);
        }

        @Override
        public Map<Long, Boolean> link(String key, long source,
                Collection<Long> destinations) {
            return invoke("link", String.class, long.class, Collection.class)
                    .with(key, source, destinations);
        }

        @Override
        public boolean link(String key, long source, long destination) {
            return invoke("link", String.class, long.class, long.class).with(
                    key, source, destination);
        }

        @Override
        public Map<Long, Boolean> ping(Collection<Long> records) {
            return invoke("ping", Collection.class).with(records);
        }

        @Override
        public boolean ping(long record) {
            return invoke("ping", long.class).with(record);
        }

        @Override
        public <T> Map<Long, Boolean> remove(String key, T value,
                Collection<Long> records) {
            return invoke("remove", String.class, Object.class,
                    Collection.class).with(key, value, records);
        }

        @Override
        public <T> boolean remove(String key, T value, long record) {
            return invoke("remove", String.class, Object.class, long.class)
                    .with(key, value, record);
        }

        @Override
        public void revert(Collection<String> keys, Collection<Long> records,
                Timestamp timestamp) {
            invoke("revert", Collection.class, Collection.class,
                    Timestamp.class).with(keys, records, timestamp);

        }

        @Override
        public void revert(Collection<String> keys, long record,
                Timestamp timestamp) {
            invoke("revert", String.class, long.class, Timestamp.class).with(
                    keys, record, timestamp);

        }

        @Override
        public void revert(String key, Collection<Long> records,
                Timestamp timestamp) {
            invoke("revert", String.class, Collection.class, Timestamp.class)
                    .with(key, records, timestamp);

        }

        @Override
        public void revert(String key, long record, Timestamp timestamp) {
            invoke("revert", String.class, long.class, Timestamp.class).with(
                    key, record, timestamp);

        }

        @Override
        public Set<Long> search(String key, String query) {
            return invoke("search", String.class, String.class)
                    .with(key, query);
        }

        @Override
        public Map<Long, Map<String, Set<Object>>> select(
                Collection<Long> records) {
            return invoke("select", Collection.class).with(records);
        }

        @Override
        public Map<Long, Map<String, Set<Object>>> select(
                Collection<Long> records, Timestamp timestamp) {
            return invoke("select", Collection.class, Timestamp.class).with(
                    records, timestamp);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                Collection<String> keys, Collection<Long> records) {
            return invoke("select", Collection.class, Collection.class).with(
                    keys, records);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                Collection<String> keys, Collection<Long> records,
                Timestamp timestamp) {
            return invoke("select", Collection.class, Collection.class,
                    Timestamp.class).with(keys, records, timestamp);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                Collection<String> keys, Criteria criteria) {
            return invoke("select", Collection.class, Criteria.class).with(
                    keys, criteria);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                Collection<String> keys, Criteria criteria, Timestamp timestamp) {
            return invoke("select", Collection.class, Criteria.class,
                    Timestamp.class).with(keys, criteria, timestamp);
        }

        @Override
        public <T> Map<String, Set<T>> select(Collection<String> keys,
                long record) {
            return invoke("select", Collection.class, long.class).with(keys,
                    record);
        }

        @Override
        public <T> Map<String, Set<T>> select(Collection<String> keys,
                long record, Timestamp timestamp) {
            return invoke("select", Collection.class, long.class,
                    Timestamp.class).with(keys, record, timestamp);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                Collection<String> keys, Object criteria) {
            return invoke("select", Collection.class, Object.class).with(keys,
                    criteria);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                Collection<String> keys, Object criteria, Timestamp timestamp) {
            return invoke("select", Collection.class, Object.class,
                    Timestamp.class).with(keys, criteria, timestamp);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                Collection<String> keys, String ccl) {
            return invoke("select", Collection.class, String.class).with(keys,
                    ccl);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                Collection<String> keys, String ccl, Timestamp timestamp) {
            return invoke("select", Collection.class, String.class,
                    Timestamp.class).with(keys, ccl, timestamp);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(Criteria criteria) {
            return invoke("select", Criteria.class).with(criteria);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(Criteria criteria,
                Timestamp timestamp) {
            return invoke("select", Criteria.class, Timestamp.class).with(
                    criteria, timestamp);
        }

        @Override
        public Map<String, Set<Object>> select(long record) {
            return invoke("select", long.class).with(record);
        }

        @Override
        public Map<String, Set<Object>> select(long record, Timestamp timestamp) {
            return invoke("select", long.class, Timestamp.class).with(record,
                    timestamp);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(Object criteria) {
            return invoke("select", Object.class).with(criteria);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(Object criteria,
                Timestamp timestamp) {
            return invoke("select", Object.class, Timestamp.class).with(
                    criteria, timestamp);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(String ccl) {
            return invoke("select", String.class).with(ccl);
        }

        @Override
        public <T> Map<Long, Set<T>> select(String key, Collection<Long> records) {
            return invoke("select", String.class, Collection.class).with(key,
                    records);
        }

        @Override
        public <T> Map<Long, Set<T>> select(String key,
                Collection<Long> records, Timestamp timestamp) {
            return invoke("select", String.class, Collection.class,
                    Timestamp.class).with(key, records, timestamp);
        }

        @Override
        public <T> Map<Long, Set<T>> select(String key, Criteria criteria) {
            return invoke("select", String.class, Criteria.class).with(key,
                    criteria);
        }

        @Override
        public <T> Map<Long, Set<T>> select(String key, Criteria criteria,
                Timestamp timestamp) {
            return invoke("select", String.class, Criteria.class,
                    Timestamp.class).with(key, criteria, timestamp);
        }

        @Override
        public <T> Set<T> select(String key, long record) {
            return invoke("select", String.class, long.class).with(key, record);
        }

        @Override
        public <T> Set<T> select(String key, long record, Timestamp timestamp) {
            return invoke("select", String.class, long.class, Timestamp.class)
                    .with(key, record, timestamp);
        }

        @Override
        public <T> Map<Long, Set<T>> select(String key, Object criteria) {
            return invoke("select", String.class, Object.class).with(key,
                    criteria);
        }

        @Override
        public <T> Map<Long, Set<T>> select(String key, Object criteria,
                Timestamp timestamp) {
            return invoke("select", String.class, Object.class, Timestamp.class)
                    .with(key, criteria, timestamp);
        }

        @Override
        public <T> Map<Long, Set<T>> select(String key, String ccl) {
            return invoke("select", String.class, String.class).with(key, ccl);
        }

        @Override
        public <T> Map<Long, Set<T>> select(String key, String ccl,
                Timestamp timestamp) {
            return invoke("select", String.class, String.class, Timestamp.class)
                    .with(key, ccl, timestamp);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(String ccl,
                Timestamp timestamp) {
            return invoke("select", String.class, Timestamp.class).with(ccl,
                    timestamp);
        }

        @Override
        public void set(String key, Object value, Collection<Long> records) {
            invoke("set", String.class, Object.class, Collection.class).with(
                    key, value, records);
        }

        @Override
        public <T> void set(String key, T value, long record) {
            invoke("set", String.class, Object.class, long.class).with(key,
                    value, record);

        }

        @Override
        public void stage() {
            invoke("stage").with();

        }

        @Override
        public Timestamp time() {
            return invoke("time").with();
        }

        @Override
        public Timestamp time(String phrase) {
            return invoke("time", String.class).with(phrase);
        }

        @Override
        public boolean unlink(String key, long source, long destination) {
            return invoke("unlink", String.class, long.class, long.class).with(
                    key, source, destination);
        }

        @Override
        public boolean verify(String key, Object value, long record) {
            return invoke("verify", String.class, Object.class, long.class)
                    .with(key, value, record);
        }

        @Override
        public boolean verify(String key, Object value, long record,
                Timestamp timestamp) {
            return invoke("audit", String.class, Object.class, long.class,
                    Timestamp.class).with(key, value, record, timestamp);
        }

        @Override
        public boolean verifyAndSwap(String key, Object expected, long record,
                Object replacement) {
            return invoke("verifyAndSwap", String.class, Object.class,
                    long.class, Object.class).with(key, expected, record,
                    replacement);
        }

        @Override
        public void verifyOrSet(String key, Object value, long record) {
            invoke("verifyOrSet", String.class, Object.class, long.class).with(
                    key, value, record);
        }

        /**
         * Return an invocation wrapper for the named {@code method} with the
         * specified {@code parameterTypes}.
         * 
         * @param method
         * @param parameterTypes
         * @return the invocation wrapper
         */
        private MethodProxy invoke(String method, Class<?>... parameterTypes) {
            try {
                for (int i = 0; i < parameterTypes.length; i++) {
                    // NOTE: We must search through each of the parameterTypes
                    // to see if they should be loaded from the server's
                    // classpath.
                    if(parameterTypes[i] == Timestamp.class) {
                        parameterTypes[i] = loader.loadClass(packageBase
                                + "Timestamp");
                    }
                    else if(parameterTypes[i] == Operator.class) {
                        parameterTypes[i] = loader.loadClass(packageBase
                                + "thrift.Operator");
                    }
                    else if(parameterTypes[i] == Criteria.class) {
                        parameterTypes[i] = loader.loadClass(packageBase
                                + "lang.Criteria");
                    }
                    else {
                        continue;
                    }
                }
                return new MethodProxy(clazz.getMethod(method, parameterTypes));
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }

        /**
         * A wrapper around a {@link Method} object that funnels all invocations
         * to the {@link #delegate}.
         * 
         * @author jnelson
         */
        private class MethodProxy {

            /**
             * The method to invoke.
             */
            Method method;

            /**
             * Construct a new instance.
             * 
             * @param method
             */
            public MethodProxy(Method method) {
                this.method = method;
            }

            /**
             * Invoke the wrapped method against the {@link #delegate} with the
             * specified args.
             * 
             * @param args
             * @return the result of invocation
             */
            @SuppressWarnings("unchecked")
            public <T> T with(Object... args) {
                try {
                    for (int i = 0; i < args.length; i++) {
                        // NOTE: We must go through each of the args to see if
                        // they must be converted to an object that is loaded
                        // from the server's classpath.
                        if(args[i] instanceof Timestamp) {
                            Timestamp obj = (Timestamp) args[i];
                            args[i] = loader
                                    .loadClass(packageBase + "Timestamp")
                                    .getMethod("fromMicros", long.class)
                                    .invoke(null, obj.getMicros());
                        }
                        else if(args[i] instanceof Operator) {
                            Operator obj = (Operator) args[i];
                            args[i] = loader
                                    .loadClass(packageBase + "thrift.Operator")
                                    .getMethod("findByValue", int.class)
                                    .invoke(null, obj.ordinal() + 1);
                        }
                        else if(args[i] instanceof Criteria) {
                            Criteria obj = (Criteria) args[i];
                            Field symbolField = Criteria.class
                                    .getDeclaredField("symbols");
                            symbolField.setAccessible(true);
                            List<Symbol> symbols = (List<Symbol>) symbolField
                                    .get(obj);
                            Class<?> rclazz = loader.loadClass(packageBase
                                    + "lang.Criteria");
                            Constructor<?> rconstructor = rclazz
                                    .getDeclaredConstructor();
                            rconstructor.setAccessible(true);
                            Object robj = rconstructor.newInstance();
                            Method rmethod = rclazz.getDeclaredMethod(
                                    "add",
                                    loader.loadClass(packageBase
                                            + "lang.Symbol"));
                            rmethod.setAccessible(true);
                            for (Symbol symbol : symbols) {
                                Object rsymbol = null;
                                if(symbol instanceof Enum) {
                                    rsymbol = loader
                                            .loadClass(
                                                    symbol.getClass().getName())
                                            .getMethod("valueOf", String.class)
                                            .invoke(null,
                                                    ((Enum<?>) symbol).name());
                                }
                                else {
                                    Method symFactory = loader.loadClass(
                                            symbol.getClass().getName())
                                            .getMethod("parse", String.class);
                                    symFactory.setAccessible(true);
                                    rsymbol = symFactory.invoke(null,
                                            symbol.toString());
                                }
                                rmethod.invoke(robj, rsymbol);
                            }
                            args[i] = robj;
                        }
                        else {
                            continue;
                        }
                    }
                    return (T) method.invoke(delegate, args);
                }
                catch (Exception e) {
                    throw Throwables.propagate(e);
                }
            }

        }

    }

}
