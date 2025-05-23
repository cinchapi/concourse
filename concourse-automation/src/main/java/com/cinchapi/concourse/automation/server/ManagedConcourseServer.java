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
package com.cinchapi.concourse.automation.server;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.SocketException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import jline.TerminalFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;

import com.cinchapi.ccl.grammar.Symbol;
import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.common.base.Array;
import com.cinchapi.common.base.ArrayBuilder;
import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.common.process.Processes;
import com.cinchapi.common.process.Processes.ProcessResult;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.Calculator;
import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.DuplicateEntryException;
import com.cinchapi.concourse.Link;
import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.automation.developer.ConcourseArtifacts;
import com.cinchapi.concourse.config.ConcourseClientConfiguration;
import com.cinchapi.concourse.config.ConcourseServerConfiguration;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.lang.sort.OrderComponent;
import com.cinchapi.concourse.thrift.Diff;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.FileOps;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * A controller for an external Concourse Server process for programmatic
 * control in other applications.
 *
 * @author Jeff Nelson
 */
public final class ManagedConcourseServer {

    /**
     * Create an {@link ManagedConcourseServer} from the {@code installer}.
     * 
     * @param installer
     * @return the ManagedConcourseServer
     */
    public static ManagedConcourseServer install(Path installer) {
        return install(installer, getNewInstallationDirectory());
    }

    /**
     * Create an {@link ManagedConcourseServer} from the {@code installer} in
     * {@code directory}.
     * 
     * @param installer
     * @param directory
     * @return the ManagedConcourseServer
     */
    public static ManagedConcourseServer install(Path installer,
            Path directory) {
        try {
            Files.createDirectories(directory);
            Path binary = directory.resolve(TARGET_BINARY_NAME);
            Files.deleteIfExists(binary);
            Files.copy(installer, binary);
            ProcessBuilder builder = new ProcessBuilder(Lists.newArrayList("sh",
                    binary.toString(), "--", "skip-integration"));
            builder.directory(directory.toFile());
            builder.redirectErrorStream();
            AtomicBoolean terminated = new AtomicBoolean(false);
            Process proc1 = builder.start();
            Stopwatch watch = Stopwatch.createStarted();
            new Thread(() -> {
                // The concourse-server installer prompts for an admin password
                // in order to complete optional system wide integration.
                // Concourse versions >= 0.5.0 have a skip-integration flag that
                // skips the prompt. Since older versions don't support the
                // prompt, we have to "kill" the process, otherwise the server
                // install will hang.
                while (!terminated.get()) {
                    if(watch.elapsed(TimeUnit.SECONDS) > 10) {
                        proc1.destroy();
                        watch.stop();
                    }
                    else {
                        log.debug("Waiting for server install to finish...");
                        try {
                            Thread.sleep(1000);
                        }
                        catch (InterruptedException e) {}
                        continue;
                    }
                }
            }).start();
            proc1.waitFor();
            terminated.set(true);
            TerminalFactory.get().restore();
            Path application = directory.resolve("concourse-server"); // the
                                                                      // install
                                                                      // directory
                                                                      // for the
                                                                      // concourse-server
                                                                      // application
            Process proc2 = Runtime.getRuntime().exec("ls " + application);
            List<String> output = Processes.getStdOut(proc2);
            if(!output.isEmpty()) {
                // delete the dev prefs because those would take precedence over
                // what is configured in this class
                Files.deleteIfExists(application.resolve(CONF)
                        .resolve("concourse.prefs.dev"));
                Files.deleteIfExists(application.resolve(CONF)
                        .resolve("concourse.yaml.dev"));
                configure(application);
                log.info("Successfully installed server in {}", application);
                return new ManagedConcourseServer(application);
            }
            else {
                throw new RuntimeException(MessageFormat.format(
                        "Unsuccesful attempt to " + "install server at {0} "
                                + "using binary from {1}",
                        directory, installer));
            }

        }
        catch (Exception e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
    }

    /**
     * Create an {@link ManagedConcourseServer} at {@code version}.
     * 
     * @param version
     * @return the ManagedConcourseServer
     */
    public static ManagedConcourseServer install(String version) {
        return install(version, getNewInstallationDirectory());
    }

    /**
     * Create an {@link ManagedConcourseServer} at {@code version} in
     * {@code directory}.
     * 
     * @param version
     * @param directory
     * @return the ManagedConcourseServer
     */
    public static ManagedConcourseServer install(String version,
            Path directory) {
        Path installer = ConcourseArtifacts.installer(version);
        return install(installer, directory);
    }

    /**
     * Return a {@link ManagedConcourseServer} that controls an
     * <strong>existing</strong> Concourse Server installation in the
     * {@code directory}.
     * 
     * @param directory
     * @return the {@link ManagedConcourseServer}
     */
    public static ManagedConcourseServer open(Path directory) {
        return new ManagedConcourseServer(directory);
    }

    /**
     * Tweak some of the preferences to make this more palatable for testing
     * (i.e. reduce the possibility of port conflicts, etc).
     * 
     * @param installDirectory
     */
    private static void configure(Path directory) {
        Path conf = directory.resolve(CONF);
        Path[] configPaths = Array.containing(conf.resolve("concourse.prefs"),
                conf.resolve("concourse.yaml"));
        ConcourseServerConfiguration config = ConcourseServerConfiguration
                .from(configPaths);
        Path data = directory.resolve("data");
        config.setBufferDirectory(data.resolve("buffer").toString());
        config.setDatabaseDirectory(data.resolve("database").toString());
        config.setClientPort(getOpenPort());
        config.setJmxPort(getOpenPort());
        config.setLogLevel(Level.DEBUG);
        config.setShutdownPort(getOpenPort());
    }

    /**
     * Collect and return all the {@code jar} files that are located in the
     * directory at {@code path}. If {@code path} is not a directory, but is
     * instead, itself, a jar file, then return a list that contains in.
     * 
     * @param path
     * @return the list of jar file URL paths
     */
    private static URL[] gatherJars(Path path) {
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
    private static void gatherJars(Path path, List<URL> jars) {
        try {
            if(Files.isDirectory(path)) {
                try (DirectoryStream<Path> stream = Files
                        .newDirectoryStream(path)) {
                    for (Path p : stream) {
                        gatherJars(p, jars);
                    }
                }

            }
            else if(path.toString().endsWith(".jar")) {
                jars.add(new URL("file://" + path.toString()));
            }
        }
        catch (IOException e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
    }

    /**
     * Return the {@link Path} to a new directory where a new Concourse Server
     * installation can be placed.
     * 
     * @return the new installation directory
     */
    private static Path getNewInstallationDirectory() {
        return DEFAULT_INSTALL_HOME.resolve(Long.toString(Time.now()));
    }

    /**
     * Get an open port.
     * 
     * @return the port
     */
    private static int getOpenPort() {
        int min = 49512;
        int max = 65535;
        int port = min + RAND.nextInt(max - min);
        return isPortAvailable(port) ? port : getOpenPort();
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
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
    }

    private static final String BIN = "bin";

    // ---relative paths
    private static final String CONF = "conf";

    /**
     * The default location where the the test server is installed if a
     * particular location is not specified.
     */
    private static final Path DEFAULT_INSTALL_HOME = Paths
            .get(FileOps.getUserHome(), ".concourse-testing");

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
     * The names of client config files.
     */
    private static final String[] CLIENT_CONFIG_FILENAMES = new String[] {
            "concourse_client.prefs", "concourse_client.yaml" };

    /**
     * A flag that determines how the client configuration files should be
     * handled when this server is {@link #destroy() destroyed}. Generally,
     * nothing is done to the configuration files unless
     * {@link #syncDefaultClientConnectionInfo()} was called by the client.
     */
    private ClientConfigCleanupAction clientConfigCleanupAction = ClientConfigCleanupAction.NONE;

    /**
     * The file whose existence determines whether or not this server should be
     * destroyed on exit.
     */
    private final Path destroyOnExitFlag;

    /**
     * The server application install directory;
     */
    private final Path directory;

    /**
     * A connection to the remote MBean server running in the managed
     * concourse-server process.
     */
    private MBeanServerConnection mBeanServerConnection = null;

    /**
     * The handler for the server's configuration.
     */
    private final ConcourseServerConfiguration config;

    /**
     * Construct a new instance.
     * 
     * @param installDirectory
     */
    private ManagedConcourseServer(Path directory) {
        this.directory = directory;
        Path conf = directory.resolve(CONF);
        // @formatter:off
        Path[] configPaths = Array.containing(
                conf.resolve("concourse.prefs"),
                conf.resolve("concourse.yaml"));
        // @formatter:on
        this.config = ConcourseServerConfiguration.from(configPaths);
        config.setLogLevel(Level.DEBUG);
        this.destroyOnExitFlag = directory.resolve(".destroyOnExit");
        setDestroyOnExit(true);
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

            @Override
            public void run() {
                if(destroyOnExit()) {
                    destroy();
                }
            }

        }));
    }

    /**
     * Return the {@link ManagedConcourseServer server's}
     * {@link ConourseServerConfiguration configuration}.
     * 
     * @return the {@link ConourseServerPreferences configuration}.
     */
    public ConcourseServerConfiguration config() {
        return config;
    }

    /**
     * Connect using the default "admin" credentials return a {@link Concourse}
     * client.
     * 
     * @return the {@link Concourse} client
     */
    public Concourse connect() {
        return connect("admin", "admin");
    }

    /**
     * Connect with the specific {@code username} and {@code password} and
     * return a {@link Concourse} client.
     * 
     * @param username
     * @param password
     * @return the {@link Concourse} client
     */
    public Concourse connect(String username, String password) {
        return new Client(username, password);
    }

    /**
     * Connect to {@code environment} with the specific {@code username} and
     * {@code password} and return a {@link Concourse} client.
     * 
     * @param username
     * @param password
     * @param environment
     * @return the {@link Concourse} client
     */
    public Concourse connect(String username, String password,
            String environment) {
        return new Client(username, password, environment);
    }

    /**
     * Stop the server, if it is running, and permanently delete the application
     * files and associated data.
     */
    public void destroy() {
        if(Files.exists(directory)) { // check if server has
                                      // been manually
                                      // destroyed
            if(isRunning()) {
                stop();
            }
            try {
                for (String filename : CLIENT_CONFIG_FILENAMES) {
                    Path file = Paths.get(filename).toAbsolutePath();
                    if(clientConfigCleanupAction == ClientConfigCleanupAction.RESTORE_BACKUP) {
                        Path backup = Paths.get(filename + ".bak")
                                .toAbsolutePath();
                        Files.move(backup, file,
                                StandardCopyOption.REPLACE_EXISTING);
                        log.info(
                                "Restored original client configuration from {} to {}",
                                backup, file);
                    }
                    else if(clientConfigCleanupAction == ClientConfigCleanupAction.DELETE) {
                        Files.delete(file);
                        log.info("Deleted client configuration from {}", file);
                    }
                }
                deleteDirectory(directory.getParent().toString());
                log.info("Deleted server install directory at {}", directory);
            }
            catch (Exception e) {
                throw CheckedExceptions.wrapAsRuntimeException(e);
            }
        }

    }

    /**
     * Return {@code true} if this server should be destroyed when the JVM
     * exits.
     * 
     * @return whether the server should be destroyed or not when the JVM exits
     */
    public synchronized boolean destroyOnExit() {
        return Files.exists(destroyOnExitFlag);
    }

    /**
     * Set a flag that determines whether this instance will be destroyed on
     * exit.
     * 
     * @param destroyOnExit
     */
    public synchronized void setDestroyOnExit(boolean destroyOnExit) {
        try {
            if(destroyOnExit) {
                Files.write(destroyOnExitFlag, new byte[] { 1 });
            }
            else {
                Files.deleteIfExists(destroyOnExitFlag);
            }
        }
        catch (IOException e) {
            throw CheckedExceptions.throwAsRuntimeException(e);
        }
    }

    /**
     * Return the {@link Path} to the directory where the
     * {@link ManagedConcoursServer} is installed.
     * 
     * @return the install directory
     */
    public Path directory() {
        return directory;
    }

    /**
     * Execute the specified {@code cli} with the provided {@code args}.
     * <p>
     * This is the equivalent of calling {@code concourse <cli> <args>}
     * on the command line
     * </p>
     * 
     * @param cli the name of the CLI to execute
     * @param args the args to pass to the cli
     * @return the standard output from executing the cli
     */
    public List<String> executeCli(String cli, String... args) {
        try {
            ArrayBuilder<String> args0 = ArrayBuilder.builder();
            args0.add("./concourse");
            args0.add(cli);
            for (String arg : args) {
                args0.add(arg.split("\\s"));
            }
            Process process = new ProcessBuilder(args0.build())
                    .directory(directory.resolve(BIN).toFile()).start();
            ProcessResult result = Processes.waitFor(process);
            if(result.exitCode() == 0) {
                return result.out();
            }
            else {
                log.warn("An error occurred executing '{}': {}", cli,
                        result.err());
                return result.err();
            }
        }
        catch (IOException e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
    }

    /**
     * Return the directory where the server stores buffer files.
     * 
     * @return the buffer directory
     */
    public Path getBufferDirectory() {
        return Paths.get(config.getBufferDirectory());
    }

    /**
     * Return the client port for this server.
     * 
     * @return the client port
     */
    public int getClientPort() {
        return config.getClientPort();
    }

    /**
     * Return the directory where the server stores database files.
     * 
     * @return the database directory
     */
    public Path getDatabaseDirectory() {
        return Paths.get(config.getDatabaseDirectory());
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
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
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
                                + config.getJmxPort() + "/jmxrmi");
                JMXConnector connector = JMXConnectorFactory.connect(url);
                mBeanServerConnection = connector.getMBeanServerConnection();
            }
            catch (Exception e) {
                throw CheckedExceptions.wrapAsRuntimeException(e);
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
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
    }

    /**
     * Return {@code true} if the default environment has writes that in its
     * buffer that are transportable to its database.
     * 
     * @return {@code true} if there are writes to transport in the default
     *         environment
     */
    public boolean hasWritesToTransport() {
        return hasWritesToTransport(config.getDefaultEnvironment());
    }

    /**
     * Return {@code true} if the {@code environment} has writes that in its
     * buffer that are transportable to its database.
     * 
     * @return {@code true} if there are writes to transport in the
     *         {@code environment}
     */
    public boolean hasWritesToTransport(String environment) {
        Path path = getBufferDirectory().resolve(environment);
        try {
            return Files.list(path).filter(p -> p.toString().endsWith(".buf"))
                    .count() > 1;
        }
        catch (IOException e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
    }

    /**
     * Install the plugin(s) contained in the {@code bundle} on this
     * {@link ManagedConcourseServer}.
     * 
     * @param bundle the path to the plugin bundle
     * @return {@code true} if the plugin(s) from the bundle is/are installed
     */
    public boolean installPlugin(Path bundle) {
        log.info("Attempting to install plugins from {}", bundle);
        List<String> out = executeCli("plugin", "install", bundle.toString(),
                "--username admin", "--password admin");
        for (String line : out) {
            if(line.contains("Successfully installed")) {
                return true;
            }
        }
        throw new RuntimeException(AnyStrings
                .format("Unable to install plugin '{}': {}", bundle, out));

    }

    /**
     * Return {@code true} if the server is currently running.
     * 
     * @return {@code true} if the server is running
     */
    public boolean isRunning() {
        return Iterables.get(execute("concourse", "status"), 0)
                .contains("is running");
    }

    /**
     * Print the content of the log file with {@code name} to the console.
     * 
     * @param name the name of the log file (i.e. console)
     */
    public void printLog(String name) {
        // NOTE: This method does not currently print contents of archived log
        // files. This is intentional because we assume that any interesting log
        // information that needs to be printed will be in the most recent file.
        Path logs = directory.resolve("log");
        Path file = logs.resolve(name + ".log");
        String content = FileOps.read(file.toString());
        System.err.println(file);
        for (int i = 0; i < file.toString().length(); ++i) {
            System.err.print('-');
        }
        System.err.println();
        System.err.println(content);
    }

    /**
     * Print the content of the log files for each of the log {@code levels} to
     * the console.
     * 
     * @param levels the log levels to print
     */
    public void printLogs(LogLevel... levels) {
        for (LogLevel level : levels) {
            String name = level.name().toLowerCase();
            printLog(name);
        }
    }

    /**
     * Restart the server.
     */
    public void restart() {
        stop();
        start();
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
            throw CheckedExceptions.wrapAsRuntimeException(e);
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
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }

    }

    /**
     * Copy the connection information for this managed server to a client
     * configuration file located in the root of the working directory so that
     * source code relying on the default connection behaviour will properly
     * connect to this server.
     * <p>
     * A test case that uses an indirect connection to Concourse (i.e. the test
     * case doesn't directly use the provided {@code client} variable provided
     * by the framework, but uses classes from the application source code that
     * has its own mechanism for connecting to Concourse) SHOULD call this
     * method so that the application code will connect to the this server for
     * the purpose of the unit test.
     * </p>
     * <p>
     * Any connection information that is synchronized will be cleaned up after
     * the test. If a prefs file already existed in the root of the working
     * directory, that file is backed up and restored so that the application
     * can run normally outside of the test cases.
     * </p>
     */
    public void syncDefaultClientConnectionInfo() {
        try {
            ArrayBuilder<Path> builder = ArrayBuilder.builder();
            for (String filename : CLIENT_CONFIG_FILENAMES) {
                Path file = Paths.get(filename).toAbsolutePath();
                builder.add(file);
                if(Files.exists(file)) {
                    Path backup = Paths.get(filename + ".bak").toAbsolutePath();
                    Files.move(file, backup);
                    clientConfigCleanupAction = ClientConfigCleanupAction.RESTORE_BACKUP;
                    log.info(
                            "Took backup for client configuration file located at "
                                    + "{}. The backup is stored in {}",
                            file, backup);
                }
                else {
                    clientConfigCleanupAction = ClientConfigCleanupAction.DELETE;
                }
            }
            Path[] files = builder.build();
            for (Path file : files) {
                FileOps.touch(file.toString());
            }
            log.info("Synchronizing the managed server's connection "
                    + "information to the client configuration files at {}",
                    Arrays.toString(files));
            ConcourseClientConfiguration config = ConcourseClientConfiguration
                    .from(files);
            config.setPort(getClientPort());
            config.setUsername("admin");
            config.setPassword("admin".toCharArray());
        }
        catch (IOException e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
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
            throw CheckedExceptions.wrapAsRuntimeException(e);
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
            String command = "bash " + cli;
            for (String arg : args) {
                command += " " + arg;
            }
            Process process = Runtime.getRuntime().exec(command, null,
                    directory.resolve(BIN).toFile());
            process.waitFor();
            if(process.exitValue() == 0) {
                return Processes.getStdOut(process);
            }
            else {
                log.warn("An error occurred executing '{}': {}", command,
                        Processes.getStdErr(process));
                return Collections.emptyList();
            }
        }
        catch (Exception e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
    }

    /**
     * Enum for log levels that can be passed to the
     * {@link #printLogs(LogLevel...)} method
     * 
     * @author Jeff Nelson
     */
    public enum LogLevel {
        CONSOLE, DEBUG, ERROR, INFO, WARN
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
            this(username, password, "");
        }

        /**
         * Construct a new instance.
         * 
         * @param username
         * @param password
         * @param environment
         */
        public Client(String username, String password, String environment) {
            this(username, password, environment, 5);
        }

        /**
         * Constructor for {@link #copyConnection()}.
         * 
         * @param clazz
         * @param delegate
         * @param loader
         */
        private Client(Class<?> clazz, Object delegate, ClassLoader loader) {
            this.clazz = clazz;
            this.delegate = delegate;
            this.loader = loader;
        }

        /**
         * Construct a new instance.
         * 
         * @param username
         * @param password
         * @param retries
         */
        private Client(String username, String password, String environment,
                int retries) {
            Object delegate = null;
            while (retries > 0 && delegate == null) {
                --retries;
                try {
                    this.loader = new URLClassLoader(gatherJars(directory()),
                            null);
                    try {
                        clazz = loader.loadClass(packageBase + "Concourse");
                    }
                    catch (ClassNotFoundException e) {
                        // Prior to version 0.5.0, Concourse classes were
                        // located in the "org.cinchapi.concourse" package, so
                        // we attempt to use that if the default does not work.
                        packageBase = "org.cinchapi.concourse.";
                        clazz = loader.loadClass(packageBase + "Concourse");
                    }
                    delegate = clazz
                            .getMethod("connect", String.class, int.class,
                                    String.class, String.class, String.class)
                            .invoke(null, "localhost", getClientPort(),
                                    username, password, environment);
                }
                catch (InvocationTargetException e) {
                    Throwable target = e.getTargetException();
                    if(target.getMessage().contains(
                            "Could not connect to the Concourse Server")) {
                        // There is a race condition where the CLI reports the
                        // server has started (because the process has
                        // registered a PID) but the thrift server hasn't been
                        // opened to accept connections yet. This logic tries to
                        // get around that by retrying the connection a handful
                        // of times before failing.
                        try {
                            Thread.sleep(5000);
                            continue;
                        }
                        catch (InterruptedException t) {/* ignore */}
                    }
                    else {
                        throw CheckedExceptions.throwAsRuntimeException(target);
                    }
                }
                catch (Exception e) {
                    throw CheckedExceptions.throwAsRuntimeException(e);
                }
            }
            if(delegate == null) {
                throw new RuntimeException(
                        "Could not connect to server before timeout...");
            }
            this.delegate = delegate;
        }

        @Override
        public void abort() {
            invoke("abort").with();
        }

        @Override
        public <T> long add(String key, T value) {
            return invoke("add", String.class, Object.class).with(key, value);
        }

        @Override
        public <T> Map<Long, Boolean> add(String key, T value,
                Collection<Long> records) {
            return invoke("add", String.class, Object.class, Collection.class)
                    .with(key, value, records);
        }

        @Override
        public <T> boolean add(String key, T value, long record) {
            return invoke("add", String.class, Object.class, long.class)
                    .with(key, value, record);
        }

        @Override
        public Map<String, Map<Object, Set<Long>>> browse(
                Collection<String> keys) {
            return invoke("browse", Collection.class, Object.class).with(keys);
        }

        @Override
        public Map<String, Map<Object, Set<Long>>> browse(
                Collection<String> keys, Timestamp timestamp) {
            return invoke("browse", Collection.class, Timestamp.class)
                    .with(keys, timestamp);
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
        public final Calculator calculate() {
            throw new UnsupportedOperationException();
        }

        @Override
        public final Calculator calculate(String method, Object... args) {
            throw new UnsupportedOperationException();
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
        public <T> Map<Timestamp, Set<T>> chronologize(String key,
                long record) {
            return invoke("chronologize", String.class, long.class).with(key,
                    record);
        }

        @Override
        public <T> Map<Timestamp, Set<T>> chronologize(String key, long record,
                Timestamp start) {
            return invoke("chronologize", String.class, long.class,
                    Timestamp.class).with(key, record, start);
        }

        @Override
        public <T> Map<Timestamp, Set<T>> chronologize(String key, long record,
                Timestamp start, Timestamp end) {
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
        public boolean consolidate(long first, long second, long... remaining) {
            return invoke("consolidate", long.class, long.class, long[].class)
                    .with(first, second, remaining);
        }

        @Override
        public Set<String> describe() {
            return invoke("describe").with();
        }

        @Override
        public Map<Long, Set<String>> describe(Collection<Long> records) {
            return invoke("describe", Collection.class).with(records);
        }

        @Override
        public Map<Long, Set<String>> describe(Collection<Long> records,
                Timestamp timestamp) {
            return invoke("describe", Collection.class, Timestamp.class)
                    .with(records, timestamp);
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
        public Set<String> describe(Timestamp timestamp) {
            return invoke("describe", Timestamp.class).with(timestamp);
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
        public <T> Map<T, Map<Diff, Set<Long>>> diff(String key,
                Timestamp start) {
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
        public Set<Long> find(Criteria criteria, Order order) {
            return invoke("find", Criteria.class, Order.class).with(criteria,
                    order);
        }

        @Override
        public Set<Long> find(Criteria criteria, Order order, Page page) {
            return invoke("find", Criteria.class, Order.class, Page.class)
                    .with(criteria, order, page);
        }

        @Override
        public Set<Long> find(Criteria criteria, Page page) {
            return invoke("find", Criteria.class, Page.class).with(criteria,
                    page);
        }

        @Override
        public Set<Long> find(String ccl) {
            return invoke("find", String.class).with(ccl);
        }

        @Override
        public Set<Long> find(String key, Object value) {
            return invoke("find", String.class, Object.class).with(key, value);
        }

        @Override
        public Set<Long> find(String key, Object value, Order order) {
            return invoke("find", String.class, Object.class, Order.class)
                    .with(key, value, order);
        }

        @Override
        public Set<Long> find(String key, Object value, Order order,
                Page page) {
            return invoke("find", String.class, Object.class, Order.class,
                    Page.class).with(key, value, order, page);
        }

        @Override
        public Set<Long> find(String key, Object value, Page page) {
            return invoke("find", String.class, Object.class, Page.class)
                    .with(key, value, page);
        }

        @Override
        public Set<Long> find(String key, Object value, Timestamp timestamp) {
            return invoke("find", String.class, Object.class, Timestamp.class)
                    .with(key, value, timestamp);
        }

        @Override
        public Set<Long> find(String key, Object value, Timestamp timestamp,
                Order order) {
            return invoke("find", String.class, Object.class, Timestamp.class,
                    Order.class).with(key, value, timestamp, order);
        }

        @Override
        public Set<Long> find(String key, Object value, Timestamp timestamp,
                Order order, Page page) {
            return invoke("find", String.class, Object.class, Timestamp.class,
                    Order.class, Page.class).with(key, value, timestamp, order,
                            page);
        }

        @Override
        public Set<Long> find(String key, Object value, Timestamp timestamp,
                Page page) {
            return invoke("find", String.class, Object.class, Timestamp.class,
                    Page.class).with(key, value, timestamp, page);
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
                Object value2, Order order) {
            return invoke("find", String.class, Operator.class, Object.class,
                    Object.class, Order.class).with(key, operator, value,
                            value2, order);
        }

        @Override
        public Set<Long> find(String key, Operator operator, Object value,
                Object value2, Order order, Page page) {
            return invoke("find", String.class, Operator.class, Object.class,
                    Object.class, Order.class, Page.class).with(key, operator,
                            value, value2, order, page);
        }

        @Override
        public Set<Long> find(String key, Operator operator, Object value,
                Object value2, Page page) {
            return invoke("find", String.class, Operator.class, Object.class,
                    Object.class, Page.class).with(key, operator, value, value2,
                            page);
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
                Object value2, Timestamp timestamp, Order order) {
            return invoke("find", String.class, Operator.class, Object.class,
                    Object.class, Timestamp.class, Order.class).with(key,
                            operator, value, value2, timestamp, order);
        }

        @Override
        public Set<Long> find(String key, Operator operator, Object value,
                Object value2, Timestamp timestamp, Order order, Page page) {
            return invoke("find", String.class, Operator.class, Object.class,
                    Object.class, Timestamp.class, Order.class, Page.class)
                            .with(key, operator, value, value2, timestamp,
                                    order, page);
        }

        @Override
        public Set<Long> find(String key, Operator operator, Object value,
                Object value2, Timestamp timestamp, Page page) {
            return invoke("find", String.class, Operator.class, Object.class,
                    Object.class, Timestamp.class, Page.class).with(key,
                            operator, value, value2, page);
        }

        @Override
        public Set<Long> find(String key, Operator operator, Object value,
                Order order) {
            return invoke("find", String.class, Operator.class, Object.class,
                    Order.class).with(key, operator, value, order);
        }

        @Override
        public Set<Long> find(String key, Operator operator, Object value,
                Order order, Page page) {
            return invoke("find", String.class, Operator.class, Object.class,
                    Order.class, Page.class).with(key, operator, value, order,
                            page);
        }

        @Override
        public Set<Long> find(String key, Operator operator, Object value,
                Page page) {
            return invoke("find", String.class, Operator.class, Object.class,
                    Page.class).with(key, operator, value, page);
        }

        @Override
        public Set<Long> find(String key, Operator operator, Object value,
                Timestamp timestamp) {
            return invoke("find", String.class, Operator.class, Object.class,
                    Timestamp.class).with(key, operator, value, timestamp);
        }

        @Override
        public Set<Long> find(String key, Operator operator, Object value,
                Timestamp timestamp, Order order) {
            return invoke("find", String.class, Operator.class, Object.class,
                    Timestamp.class, Order.class).with(key, operator, value,
                            timestamp, order);
        }

        @Override
        public Set<Long> find(String key, Operator operator, Object value,
                Timestamp timestamp, Order order, Page page) {
            return invoke("find", String.class, Operator.class, Object.class,
                    Timestamp.class, Order.class, Page.class).with(key,
                            operator, value, timestamp, order, page);
        }

        @Override
        public Set<Long> find(String key, Operator operator, Object value,
                Timestamp timestamp, Page page) {
            return invoke("find", String.class, Operator.class, Object.class,
                    Timestamp.class, Page.class).with(key, operator, value,
                            timestamp, page);
        }

        @Override
        public Set<Long> find(String ccl, Order order) {
            return invoke("find", String.class, Order.class).with(ccl, order);
        }

        @Override
        public Set<Long> find(String ccl, Order order, Page page) {
            return invoke("find", String.class, Order.class, Page.class)
                    .with(ccl, order, page);
        }

        @Override
        public Set<Long> find(String ccl, Page page) {
            return invoke("find", String.class, Page.class).with(ccl, page);
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
                Object value2, Order order) {
            return invoke("find", String.class, String.class, Object.class,
                    Object.class, Order.class).with(key, operator, value,
                            value2, order);
        }

        @Override
        public Set<Long> find(String key, String operator, Object value,
                Object value2, Order order, Page page) {
            return invoke("find", String.class, String.class, Object.class,
                    Object.class, Order.class, Page.class).with(key, operator,
                            value, value2, order, page);
        }

        @Override
        public Set<Long> find(String key, String operator, Object value,
                Object value2, Page page) {
            return invoke("find", String.class, String.class, Object.class,
                    Object.class, Page.class).with(key, operator, value, value2,
                            page);
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
                Object value2, Timestamp timestamp, Order order) {
            return invoke("find", String.class, String.class, Object.class,
                    Object.class, Timestamp.class, Order.class).with(key,
                            operator, value, value2, timestamp, order);
        }

        @Override
        public Set<Long> find(String key, String operator, Object value,
                Object value2, Timestamp timestamp, Order order, Page page) {
            return invoke("find", String.class, String.class, Object.class,
                    Object.class, Timestamp.class, Order.class, Page.class)
                            .with(key, operator, value, value2, timestamp,
                                    order, page);
        }

        @Override
        public Set<Long> find(String key, String operator, Object value,
                Object value2, Timestamp timestamp, Page page) {
            return invoke("find", String.class, String.class, Object.class,
                    Object.class, Timestamp.class, Page.class).with(key,
                            operator, value, value2, timestamp, page);
        }

        @Override
        public Set<Long> find(String key, String operator, Object value,
                Order order) {
            return invoke("find", String.class, String.class, Object.class,
                    Order.class).with(key, operator, value, order);
        }

        @Override
        public Set<Long> find(String key, String operator, Object value,
                Order order, Page page) {
            return invoke("find", String.class, String.class, Object.class,
                    Order.class, Page.class).with(key, operator, value, order,
                            page);
        }

        @Override
        public Set<Long> find(String key, String operator, Object value,
                Page page) {
            return invoke("find", String.class, String.class, Object.class,
                    Page.class).with(key, operator, value, page);
        }

        @Override
        public Set<Long> find(String key, String operator, Object value,
                Timestamp timestamp) {
            return invoke("find", String.class, String.class, Object.class,
                    Timestamp.class).with(key, operator, value, timestamp);
        }

        @Override
        public Set<Long> find(String key, String operator, Object value,
                Timestamp timestamp, Order order) {
            return invoke("find", String.class, String.class, Object.class,
                    Timestamp.class, Order.class).with(key, operator, value,
                            timestamp, order);
        }

        @Override
        public Set<Long> find(String key, String operator, Object value,
                Timestamp timestamp, Order order, Page page) {
            return invoke("find", String.class, String.class, Object.class,
                    Timestamp.class, Order.class, Page.class).with(key,
                            operator, value, timestamp, order, page);
        }

        @Override
        public Set<Long> find(String key, String operator, Object value,
                Timestamp timestamp, Page page) {
            return invoke("find", String.class, String.class, Object.class,
                    Timestamp.class, Page.class).with(key, operator, value,
                            timestamp, page);
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
            return invoke("findOrInsert", Criteria.class, String.class)
                    .with(criteria, json);
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
                Collection<Long> records, Order order) {
            return invoke("get", Collection.class, Collection.class,
                    Order.class).with(keys, records, order);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
                Collection<Long> records, Order order, Page page) {
            return invoke("get", Collection.class, Collection.class,
                    Order.class, Page.class).with(keys, records, order, page);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
                Collection<Long> records, Page page) {
            return invoke("get", Collection.class, Collection.class, Page.class)
                    .with(keys, records, page);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
                Collection<Long> records, Timestamp timestamp) {
            return invoke("get", Collection.class, Collection.class,
                    Timestamp.class).with(keys, records, timestamp);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
                Collection<Long> records, Timestamp timestamp, Order order) {
            return invoke("get", Collection.class, Collection.class,
                    Timestamp.class, Order.class).with(keys, records, timestamp,
                            order);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
                Collection<Long> records, Timestamp timestamp, Order order,
                Page page) {
            return invoke("get", Collection.class, Collection.class,
                    Timestamp.class, Order.class, Page.class).with(keys,
                            records, timestamp, order, page);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
                Collection<Long> records, Timestamp timestamp, Page page) {
            return invoke("get", Collection.class, Collection.class,
                    Timestamp.class, Page.class).with(keys, records, timestamp,
                            page);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
                Criteria criteria) {
            return invoke("get", Collection.class, Criteria.class).with(keys,
                    criteria);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
                Criteria criteria, Order order) {
            return invoke("get", Collection.class, Criteria.class, Order.class)
                    .with(keys, criteria, order);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
                Criteria criteria, Order order, Page page) {
            return invoke("get", Collection.class, Criteria.class, Order.class,
                    Page.class).with(keys, criteria, order, page);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
                Criteria criteria, Page page) {
            return invoke("get", Collection.class, Criteria.class, Page.class)
                    .with(keys, criteria, page);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
                Criteria criteria, Timestamp timestamp) {
            return invoke("get", Collection.class, Criteria.class,
                    Timestamp.class).with(keys, criteria, timestamp);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
                Criteria criteria, Timestamp timestamp, Order order) {
            return invoke("get", Collection.class, Criteria.class,
                    Timestamp.class, Order.class).with(keys, criteria,
                            timestamp, order);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
                Criteria criteria, Timestamp timestamp, Order order,
                Page page) {
            return invoke("get", Collection.class, Criteria.class,
                    Timestamp.class, Order.class, Page.class).with(keys,
                            criteria, timestamp, order, page);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
                Criteria criteria, Timestamp timestamp, Page page) {
            return invoke("get", Collection.class, Criteria.class,
                    Timestamp.class, Page.class).with(keys, criteria, timestamp,
                            page);
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
                String ccl) {
            return invoke("get", Collection.class, String.class).with(keys,
                    ccl);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
                String ccl, Order order) {
            return invoke("get", Collection.class, String.class, Order.class)
                    .with(keys, ccl, order);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
                String ccl, Order order, Page page) {
            return invoke("get", Collection.class, String.class, Order.class,
                    Page.class).with(keys, ccl, order, page);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
                String ccl, Page page) {
            return invoke("get", Collection.class, String.class, Page.class)
                    .with(keys, ccl, page);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
                String ccl, Timestamp timestamp) {
            return invoke("get", Collection.class, String.class,
                    Timestamp.class).with(keys, ccl, timestamp);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
                String ccl, Timestamp timestamp, Order order) {
            return invoke("get", Collection.class, String.class,
                    Timestamp.class, Order.class).with(keys, ccl, timestamp,
                            order);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
                String ccl, Timestamp timestamp, Order order, Page page) {
            return invoke("get", Collection.class, String.class,
                    Timestamp.class, Order.class, Page.class).with(keys, ccl,
                            timestamp, order, page);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
                String ccl, Timestamp timestamp, Page page) {
            return invoke("get", Collection.class, String.class,
                    Timestamp.class, Page.class).with(keys, ccl, timestamp,
                            page);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(Criteria criteria) {
            return invoke("get", Criteria.class).with(criteria);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(Criteria criteria,
                Order order) {
            return invoke("get", Criteria.class, Order.class).with(criteria,
                    order);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(Criteria criteria, Order order,
                Page page) {
            return invoke("get", Criteria.class, Order.class, Page.class)
                    .with(criteria, order, page);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(Criteria criteria, Page page) {
            return invoke("get", Criteria.class, Page.class).with(criteria,
                    page);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(Criteria criteria,
                Timestamp timestamp) {
            return invoke("get", Criteria.class, Timestamp.class).with(criteria,
                    timestamp);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(Criteria criteria,
                Timestamp timestamp, Order order) {
            return invoke("get", Criteria.class, Timestamp.class, Order.class)
                    .with(criteria, timestamp, order);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(Criteria criteria,
                Timestamp timestamp, Order order, Page page) {
            return invoke("get", Criteria.class, Timestamp.class, Order.class,
                    Page.class).with(criteria, timestamp, order, page);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(Criteria criteria,
                Timestamp timestamp, Page page) {
            return invoke("get", Criteria.class, Timestamp.class, Page.class)
                    .with(criteria, timestamp, page);
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
                Order order) {
            return invoke("get", String.class, Collection.class, Order.class)
                    .with(key, records, order);
        }

        @Override
        public <T> Map<Long, T> get(String key, Collection<Long> records,
                Order order, Page page) {
            return invoke("get", String.class, Collection.class, Order.class,
                    Page.class).with(key, records, order, page);
        }

        @Override
        public <T> Map<Long, T> get(String key, Collection<Long> records,
                Page page) {
            return invoke("get", String.class, Collection.class, Page.class)
                    .with(key, records, page);
        }

        @Override
        public <T> Map<Long, T> get(String key, Collection<Long> records,
                Timestamp timestamp) {
            return invoke("get", String.class, Collection.class,
                    Timestamp.class).with(key, records, timestamp);
        }

        @Override
        public <T> Map<Long, T> get(String key, Collection<Long> records,
                Timestamp timestamp, Order order) {
            return invoke("get", String.class, Collection.class,
                    Timestamp.class, Order.class).with(key, records, timestamp,
                            order);
        }

        @Override
        public <T> Map<Long, T> get(String key, Collection<Long> records,
                Timestamp timestamp, Order order, Page page) {
            return invoke("get", String.class, Collection.class,
                    Timestamp.class, Order.class, Page.class).with(key, records,
                            timestamp, order, page);
        }

        @Override
        public <T> Map<Long, T> get(String key, Collection<Long> records,
                Timestamp timestamp, Page page) {
            return invoke("get", String.class, Collection.class,
                    Timestamp.class, Page.class).with(key, records, timestamp,
                            page);
        }

        @Override
        public <T> Map<Long, T> get(String key, Criteria criteria) {
            return invoke("get", String.class, Criteria.class).with(key,
                    criteria);
        }

        @Override
        public <T> Map<Long, T> get(String key, Criteria criteria,
                Order order) {
            return invoke("get", String.class, Criteria.class, Order.class)
                    .with(key, criteria, order);
        }

        @Override
        public <T> Map<Long, T> get(String key, Criteria criteria, Order order,
                Page page) {
            return invoke("get", String.class, Criteria.class, Order.class,
                    Page.class).with(key, criteria, order, page);
        }

        @Override
        public <T> Map<Long, T> get(String key, Criteria criteria, Page page) {
            return invoke("get", String.class, Criteria.class, Page.class)
                    .with(key, criteria, page);
        }

        @Override
        public <T> Map<Long, T> get(String key, Criteria criteria,
                Timestamp timestamp) {
            return invoke("get", String.class, Criteria.class, Timestamp.class)
                    .with(key, criteria, timestamp);
        }

        @Override
        public <T> Map<Long, T> get(String key, Criteria criteria,
                Timestamp timestamp, Order order) {
            return invoke("get", String.class, Criteria.class, Timestamp.class,
                    Order.class).with(key, criteria, timestamp, order);
        }

        @Override
        public <T> Map<Long, T> get(String key, Criteria criteria,
                Timestamp timestamp, Order order, Page page) {
            return invoke("get", String.class, Criteria.class, Timestamp.class,
                    Order.class, Page.class).with(key, criteria, timestamp,
                            order, page);
        }

        @Override
        public <T> Map<Long, T> get(String key, Criteria criteria,
                Timestamp timestamp, Page page) {
            return invoke("get", String.class, Criteria.class, Timestamp.class,
                    Page.class).with(key, criteria, timestamp, page);
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
        public <T> Map<Long, Map<String, T>> get(String ccl, Order order) {
            return invoke("get", String.class, Order.class).with(ccl, order);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(String ccl, Order order,
                Page page) {
            return invoke("get", String.class, Order.class, Page.class)
                    .with(ccl, order, page);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(String ccl, Page page) {
            return invoke("get", String.class, Page.class).with(ccl, page);
        }

        @Override
        public <T> Map<Long, T> get(String key, String ccl) {
            return invoke("get", String.class, String.class).with(key, ccl);
        }

        @Override
        public <T> Map<Long, T> get(String key, String ccl, Order order) {
            return invoke("get", String.class, String.class, Order.class)
                    .with(key, ccl, order);
        }

        @Override
        public <T> Map<Long, T> get(String key, String ccl, Order order,
                Page page) {
            return invoke("get", String.class, String.class, Order.class,
                    Page.class).with(key, ccl, order, page);
        }

        @Override
        public <T> Map<Long, T> get(String key, String ccl, Page page) {
            return invoke("get", String.class, String.class, Page.class)
                    .with(key, ccl, page);
        }

        @Override
        public <T> Map<Long, T> get(String key, String ccl,
                Timestamp timestamp) {
            return invoke("get", String.class, String.class, Timestamp.class)
                    .with(key, ccl, timestamp);
        }

        @Override
        public <T> Map<Long, T> get(String key, String ccl, Timestamp timestamp,
                Order order) {
            return invoke("get", String.class, String.class, Timestamp.class,
                    Order.class).with(key, ccl, timestamp, order);
        }

        @Override
        public <T> Map<Long, T> get(String key, String ccl, Timestamp timestamp,
                Order order, Page page) {
            return invoke("get", String.class, String.class, Timestamp.class,
                    Order.class, Page.class).with(key, ccl, timestamp, order,
                            page);
        }

        @Override
        public <T> Map<Long, T> get(String key, String ccl, Timestamp timestamp,
                Page page) {
            return invoke("get", String.class, String.class, Timestamp.class,
                    Page.class).with(key, ccl, timestamp, page);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(String ccl,
                Timestamp timestamp) {
            return invoke("get", String.class, Timestamp.class).with(ccl,
                    timestamp);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(String ccl,
                Timestamp timestamp, Order order) {
            return invoke("get", String.class, Timestamp.class, Order.class)
                    .with(ccl, timestamp, order);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(String ccl,
                Timestamp timestamp, Order order, Page page) {
            return invoke("get", String.class, Timestamp.class, Order.class,
                    Page.class).with(ccl, timestamp, order, page);
        }

        @Override
        public <T> Map<Long, Map<String, T>> get(String ccl,
                Timestamp timestamp, Page page) {
            return invoke("get", String.class, Timestamp.class, Page.class)
                    .with(ccl, timestamp, page);
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
        public Map<Long, Boolean> insert(String json,
                Collection<Long> records) {
            return invoke("insert", String.class, Collection.class).with(json,
                    records);
        }

        @Override
        public boolean insert(String json, long record) {
            return invoke("insert", String.class, long.class).with(json,
                    record);
        }

        @Override
        public Set<Long> inventory() {
            return invoke("inventory").with();
        }

        @Override
        public <T> T invokePlugin(String id, String method, Object... args) {
            return invoke("invokePlugin", String.class, String.class,
                    Object[].class).with(id, method, args);
        }

        @Override
        public String jsonify(Collection<Long> records) {
            return invoke("jsonify", Collection.class).with(records);
        }

        @Override
        public String jsonify(Collection<Long> records, boolean identifier) {
            return invoke("jsonify", Collection.class, boolean.class)
                    .with(records, identifier);
        }

        @Override
        public String jsonify(Collection<Long> records, Timestamp timestamp) {
            return invoke("jsonify", Collection.class, Timestamp.class)
                    .with(records, timestamp);
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
        public Map<Long, Boolean> link(String key,
                Collection<Long> destinations, long source) {
            return invoke("link", String.class, long.class, Collection.class)
                    .with(key, destinations, source);
        }

        @Override
        public boolean link(String key, long destination, long source) {
            return invoke("link", String.class, long.class, long.class)
                    .with(key, destination, source);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> navigate(
                Collection<String> keys, Collection<Long> records) {
            return invoke("navigate", Collection.class, Collection.class)
                    .with(keys, records);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> navigate(
                Collection<String> keys, Collection<Long> records,
                Timestamp timestamp) {
            return invoke("navigate", Collection.class, Collection.class,
                    Timestamp.class).with(keys, records, timestamp);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> navigate(
                Collection<String> keys, Criteria criteria) {
            return invoke("navigate", Collection.class, Criteria.class)
                    .with(keys, criteria);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> navigate(
                Collection<String> keys, Criteria criteria,
                Timestamp timestamp) {
            return invoke("navigate", Collection.class, Criteria.class,
                    Timestamp.class).with(keys, criteria, timestamp);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> navigate(
                Collection<String> keys, long record) {
            return invoke("navigate", Collection.class, long.class).with(keys,
                    record);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> navigate(
                Collection<String> keys, long record, Timestamp timestamp) {
            return invoke("navigate", Collection.class, long.class,
                    Timestamp.class).with(keys, record, timestamp);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> navigate(
                Collection<String> keys, String ccl) {
            return invoke("navigate", Collection.class, String.class).with(keys,
                    ccl);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> navigate(
                Collection<String> keys, String ccl, Timestamp timestamp) {
            return invoke("navigate", Collection.class, String.class,
                    Timestamp.class).with(keys, ccl, timestamp);
        }

        @Override
        public <T> Map<Long, Set<T>> navigate(String key,
                Collection<Long> records) {
            return invoke("navigate", String.class, Collection.class).with(key,
                    records);
        }

        @Override
        public <T> Map<Long, Set<T>> navigate(String key,
                Collection<Long> records, Timestamp timestamp) {
            return invoke("navigate", String.class, Collection.class,
                    Timestamp.class).with(key, records, timestamp);
        }

        @Override
        public <T> Map<Long, Set<T>> navigate(String key, Criteria criteria) {
            return invoke("navigate", String.class, String.class).with(key,
                    criteria);
        }

        @Override
        public <T> Map<Long, Set<T>> navigate(String key, Criteria criteria,
                Timestamp timestamp) {
            return invoke("navigate", String.class, Criteria.class,
                    Timestamp.class).with(key, criteria, timestamp);
        }

        @Override
        public <T> Map<Long, Set<T>> navigate(String key, long record) {
            return invoke("navigate", String.class, long.class).with(key,
                    record);
        }

        @Override
        public <T> Map<Long, Set<T>> navigate(String key, long record,
                Timestamp timestamp) {
            return invoke("navigate", String.class, long.class, Timestamp.class)
                    .with(key, record, timestamp);
        }

        @Override
        public <T> Map<Long, Set<T>> navigate(String key, String ccl) {
            return invoke("navigate", String.class, Criteria.class).with(key,
                    ccl);
        }

        @Override
        public <T> Map<Long, Set<T>> navigate(String key, String ccl,
                Timestamp timestamp) {
            return invoke("navigate", String.class, String.class,
                    Timestamp.class).with(key, ccl, timestamp);
        }

        @Override
        public boolean ping() {
            return invoke("ping").with();
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
        public <T> void reconcile(String key, long record,
                Collection<T> values) {
            invoke("reconcile", String.class, long.class, Collection.class)
                    .with(key, record, values);
        }

        @Override
        public <T> Map<Long, Boolean> remove(String key, T value,
                Collection<Long> records) {
            return invoke("remove", String.class, long.class, Collection.class)
                    .with(key, value, records);
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
            invoke("revert", String.class, long.class, Timestamp.class)
                    .with(keys, record, timestamp);

        }

        @Override
        public void revert(String key, Collection<Long> records,
                Timestamp timestamp) {
            invoke("revert", String.class, Collection.class, Timestamp.class)
                    .with(key, records, timestamp);

        }

        @Override
        public void revert(String key, long record, Timestamp timestamp) {
            invoke("revert", String.class, long.class, Timestamp.class)
                    .with(key, record, timestamp);

        }

        @Override
        public Map<Timestamp, List<String>> review(long record) {
            return invoke("review", long.class).with(record);
        }

        @Override
        public Map<Timestamp, List<String>> review(long record,
                Timestamp start) {
            return invoke("review", long.class, Timestamp.class).with(record,
                    start);
        }

        @Override
        public Map<Timestamp, List<String>> review(long record, Timestamp start,
                Timestamp end) {
            return invoke("review", long.class, Timestamp.class,
                    Timestamp.class).with(start, end);
        }

        @Override
        public Map<Timestamp, List<String>> review(String key, long record) {
            return invoke("review", String.class, long.class).with(key, record);
        }

        @Override
        public Map<Timestamp, List<String>> review(String key, long record,
                Timestamp start) {
            return invoke("review", String.class, long.class, Timestamp.class)
                    .with(key, record, start);
        }

        @Override
        public Map<Timestamp, List<String>> review(String key, long record,
                Timestamp start, Timestamp end) {
            return invoke("review", String.class, long.class, Timestamp.class,
                    Timestamp.class).with(key, record, start, end);
        }

        @Override
        public Set<Long> search(String key, String query) {
            return invoke("search", String.class, String.class).with(key,
                    query);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                Collection<Long> records) {
            return invoke("select", Collection.class).with(records);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                Collection<Long> records, Order order) {
            return invoke("select", Collection.class, Order.class).with(records,
                    order);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                Collection<Long> records, Order order, Page page) {
            return invoke("select", Collection.class, Order.class, Page.class)
                    .with(records, order, page);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                Collection<Long> records, Page page) {
            return invoke("select", Collection.class, Page.class).with(records,
                    page);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                Collection<Long> records, Timestamp timestamp) {
            return invoke("select", Collection.class, Timestamp.class)
                    .with(records, timestamp);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                Collection<Long> records, Timestamp timestamp, Order order) {
            return invoke("select", Collection.class, Timestamp.class,
                    Order.class).with(records, timestamp, order);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                Collection<Long> records, Timestamp timestamp, Order order,
                Page page) {
            return invoke("select", Collection.class, Timestamp.class,
                    Order.class, Page.class).with(records, timestamp, order,
                            page);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                Collection<Long> records, Timestamp timestamp, Page page) {
            return invoke("select", Collection.class, Timestamp.class,
                    Page.class).with(records, timestamp, page);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                Collection<String> keys, Collection<Long> records) {
            return invoke("select", Collection.class, Collection.class)
                    .with(keys, records);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                Collection<String> keys, Collection<Long> records,
                Order order) {
            return invoke("select", Collection.class, Collection.class,
                    Order.class).with(keys, records, order);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                Collection<String> keys, Collection<Long> records, Order order,
                Page page) {
            return invoke("select", Collection.class, Collection.class,
                    Order.class, Page.class).with(keys, records, order, page);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                Collection<String> keys, Collection<Long> records, Page page) {
            return invoke("select", Collection.class, Collection.class,
                    Page.class).with(keys, records, page);
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
                Collection<String> keys, Collection<Long> records,
                Timestamp timestamp, Order order) {
            return invoke("select", Collection.class, Collection.class,
                    Timestamp.class, Order.class).with(keys, records, timestamp,
                            order);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                Collection<String> keys, Collection<Long> records,
                Timestamp timestamp, Order order, Page page) {
            return invoke("select", Collection.class, Collection.class,
                    Timestamp.class, Order.class, Page.class).with(keys,
                            records, timestamp, order, page);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                Collection<String> keys, Collection<Long> records,
                Timestamp timestamp, Page page) {
            return invoke("select", Collection.class, Collection.class,
                    Timestamp.class, Page.class).with(keys, records, timestamp,
                            page);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                Collection<String> keys, Criteria criteria) {
            return invoke("select", Collection.class, Criteria.class).with(keys,
                    criteria);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                Collection<String> keys, Criteria criteria, Order order) {
            return invoke("select", Collection.class, Criteria.class,
                    Order.class).with(keys, criteria, order);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                Collection<String> keys, Criteria criteria, Order order,
                Page page) {
            return invoke("select", Collection.class, Criteria.class,
                    Order.class, Page.class).with(keys, criteria, order, page);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                Collection<String> keys, Criteria criteria, Page page) {
            return invoke("select", Collection.class, Criteria.class,
                    Page.class).with(keys, criteria, page);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                Collection<String> keys, Criteria criteria,
                Timestamp timestamp) {
            return invoke("select", Collection.class, Criteria.class,
                    Timestamp.class).with(keys, criteria, timestamp);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                Collection<String> keys, Criteria criteria, Timestamp timestamp,
                Order order) {
            return invoke("select", Collection.class, Criteria.class,
                    Timestamp.class, Order.class).with(keys, criteria,
                            timestamp, order);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                Collection<String> keys, Criteria criteria, Timestamp timestamp,
                Order order, Page page) {
            return invoke("select", Collection.class, Criteria.class,
                    Timestamp.class, Order.class, Page.class).with(keys,
                            criteria, timestamp, order, page);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                Collection<String> keys, Criteria criteria, Timestamp timestamp,
                Page page) {
            return invoke("select", Collection.class, Criteria.class,
                    Timestamp.class, Page.class).with(keys, criteria, timestamp,
                            page);
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
                Collection<String> keys, String ccl) {
            return invoke("select", Collection.class, String.class).with(keys,
                    ccl);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                Collection<String> keys, String ccl, Order order) {
            return invoke("select", Collection.class, String.class, Order.class)
                    .with(keys, ccl, order);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                Collection<String> keys, String ccl, Order order, Page page) {
            return invoke("select", Collection.class, String.class, Order.class,
                    Page.class).with(keys, ccl, order, page);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                Collection<String> keys, String ccl, Page page) {
            return invoke("select", Collection.class, String.class, Page.class)
                    .with(keys, ccl, page);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                Collection<String> keys, String ccl, Timestamp timestamp) {
            return invoke("select", Collection.class, String.class,
                    Timestamp.class).with(keys, ccl, timestamp);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                Collection<String> keys, String ccl, Timestamp timestamp,
                Order order) {
            return invoke("select", Collection.class, String.class,
                    Timestamp.class, Order.class).with(keys, ccl, timestamp,
                            order);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                Collection<String> keys, String ccl, Timestamp timestamp,
                Order order, Page page) {
            return invoke("select", Collection.class, String.class, Order.class,
                    Page.class).with(keys, ccl, order, page);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                Collection<String> keys, String ccl, Timestamp timestamp,
                Page page) {
            return invoke("select", Collection.class, String.class,
                    Timestamp.class, Page.class).with(keys, ccl, timestamp,
                            page);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(Criteria criteria) {
            return invoke("select", Criteria.class).with(criteria);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(Criteria criteria,
                Order order) {
            return invoke("select", Criteria.class, Order.class).with(criteria,
                    order);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(Criteria criteria,
                Order order, Page page) {
            return invoke("select", Criteria.class, Order.class, Page.class)
                    .with(criteria, order, page);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(Criteria criteria,
                Page page) {
            return invoke("select", Criteria.class, Page.class).with(criteria,
                    page);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(Criteria criteria,
                Timestamp timestamp) {
            return invoke("select", Criteria.class, Timestamp.class)
                    .with(criteria, timestamp);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(Criteria criteria,
                Timestamp timestamp, Order order) {
            return invoke("select", Criteria.class, Timestamp.class,
                    Order.class).with(criteria, timestamp, order);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(Criteria criteria,
                Timestamp timestamp, Order order, Page page) {
            return invoke("select", Criteria.class, Timestamp.class,
                    Order.class, Page.class).with(criteria, timestamp, order,
                            page);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(Criteria criteria,
                Timestamp timestamp, Page page) {
            return invoke("select", Criteria.class, Timestamp.class, Page.class)
                    .with(criteria, timestamp, page);
        }

        @Override
        public <T> Map<String, Set<T>> select(long record) {
            return invoke("select", long.class).with(record);
        }

        @Override
        public <T> Map<String, Set<T>> select(long record,
                Timestamp timestamp) {
            return invoke("select", long.class, Timestamp.class).with(record,
                    timestamp);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(String ccl) {
            return invoke("select", String.class).with(ccl);
        }

        @Override
        public <T> Map<Long, Set<T>> select(String key,
                Collection<Long> records) {
            return invoke("select", String.class, Collection.class).with(key,
                    records);
        }

        @Override
        public <T> Map<Long, Set<T>> select(String key,
                Collection<Long> records, Order order) {
            return invoke("select", String.class, Collection.class, Order.class)
                    .with(key, records, order);
        }

        @Override
        public <T> Map<Long, Set<T>> select(String key,
                Collection<Long> records, Order order, Page page) {
            return invoke("select", String.class, Collection.class, Order.class,
                    Page.class).with(key, records, order, page);
        }

        @Override
        public <T> Map<Long, Set<T>> select(String key,
                Collection<Long> records, Page page) {
            return invoke("select", String.class, Collection.class, Page.class)
                    .with(key, records, page);
        }

        @Override
        public <T> Map<Long, Set<T>> select(String key,
                Collection<Long> records, Timestamp timestamp) {
            return invoke("select", String.class, Collection.class,
                    Timestamp.class).with(key, records, timestamp);
        }

        @Override
        public <T> Map<Long, Set<T>> select(String key,
                Collection<Long> records, Timestamp timestamp, Order order) {
            return invoke("select", String.class, Collection.class,
                    Timestamp.class, Order.class).with(key, records, timestamp,
                            order);
        }

        @Override
        public <T> Map<Long, Set<T>> select(String key,
                Collection<Long> records, Timestamp timestamp, Order order,
                Page page) {
            return invoke("select", String.class, Collection.class,
                    Timestamp.class, Order.class, Page.class).with(key, records,
                            timestamp, order, page);
        }

        @Override
        public <T> Map<Long, Set<T>> select(String key,
                Collection<Long> records, Timestamp timestamp, Page page) {
            return invoke("select", String.class, Collection.class,
                    Timestamp.class, Page.class).with(key, records, timestamp,
                            page);
        }

        @Override
        public <T> Map<Long, Set<T>> select(String key, Criteria criteria) {
            return invoke("select", String.class, Criteria.class).with(key,
                    criteria);
        }

        @Override
        public <T> Map<Long, Set<T>> select(String key, Criteria criteria,
                Order order) {
            return invoke("select", String.class, Criteria.class, Order.class)
                    .with(key, criteria, order);
        }

        @Override
        public <T> Map<Long, Set<T>> select(String key, Criteria criteria,
                Order order, Page page) {
            return invoke("select", String.class, Criteria.class, Order.class,
                    Page.class).with(key, criteria, order, page);
        }

        @Override
        public <T> Map<Long, Set<T>> select(String key, Criteria criteria,
                Page page) {
            return invoke("select", String.class, Criteria.class, Page.class)
                    .with(key, criteria, page);
        }

        @Override
        public <T> Map<Long, Set<T>> select(String key, Criteria criteria,
                Timestamp timestamp) {
            return invoke("select", String.class, Criteria.class,
                    Timestamp.class).with(key, criteria, timestamp);
        }

        @Override
        public <T> Map<Long, Set<T>> select(String key, Criteria criteria,
                Timestamp timestamp, Order order) {
            return invoke("select", String.class, Criteria.class,
                    Timestamp.class, Order.class).with(key, criteria, timestamp,
                            order);
        }

        @Override
        public <T> Map<Long, Set<T>> select(String key, Criteria criteria,
                Timestamp timestamp, Order order, Page page) {
            return invoke("select", String.class, Criteria.class,
                    Timestamp.class, Order.class, Page.class).with(key,
                            criteria, timestamp, order, page);
        }

        @Override
        public <T> Map<Long, Set<T>> select(String key, Criteria criteria,
                Timestamp timestamp, Page page) {
            return invoke("select", String.class, Criteria.class,
                    Timestamp.class, Page.class).with(key, criteria, timestamp,
                            page);
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
        public <T> Map<Long, Map<String, Set<T>>> select(String ccl,
                Order order) {
            return invoke("select", String.class, Order.class).with(ccl, order);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(String ccl,
                Order order, Page page) {
            return invoke("select", String.class, Order.class, Page.class)
                    .with(ccl, order, page);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(String ccl,
                Page page) {
            return invoke("select", String.class, Page.class).with(ccl, page);
        }

        @Override
        public <T> Map<Long, Set<T>> select(String key, String ccl) {
            return invoke("select", String.class, String.class).with(key, ccl);
        }

        @Override
        public <T> Map<Long, Set<T>> select(String key, String ccl,
                Order order) {
            return invoke("select", String.class, String.class, Order.class)
                    .with(key, ccl, order);
        }

        @Override
        public <T> Map<Long, Set<T>> select(String key, String ccl, Order order,
                Page page) {
            return invoke("select", String.class, String.class, Order.class,
                    Page.class).with(key, ccl, order, page);
        }

        @Override
        public <T> Map<Long, Set<T>> select(String key, String ccl, Page page) {
            return invoke("select", String.class, String.class, Page.class)
                    .with(key, ccl, page);
        }

        @Override
        public <T> Map<Long, Set<T>> select(String key, String ccl,
                Timestamp timestamp) {
            return invoke("select", String.class, String.class, Timestamp.class)
                    .with(key, ccl, timestamp);
        }

        @Override
        public <T> Map<Long, Set<T>> select(String key, String ccl,
                Timestamp timestamp, Order order) {
            return invoke("select", String.class, String.class, Timestamp.class,
                    Order.class).with(key, ccl, timestamp, order);
        }

        @Override
        public <T> Map<Long, Set<T>> select(String key, String ccl,
                Timestamp timestamp, Order order, Page page) {
            return invoke("select", String.class, String.class, Timestamp.class,
                    Order.class, Page.class).with(key, ccl, timestamp, order,
                            page);
        }

        @Override
        public <T> Map<Long, Set<T>> select(String key, String ccl,
                Timestamp timestamp, Page page) {
            return invoke("select", String.class, String.class, Timestamp.class,
                    Page.class).with(key, ccl, timestamp, page);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(String ccl,
                Timestamp timestamp) {
            return invoke("select", String.class, Timestamp.class).with(ccl,
                    timestamp);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(String ccl,
                Timestamp timestamp, Order order) {
            return invoke("select", String.class, Timestamp.class, Order.class)
                    .with(ccl, timestamp, order);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(String ccl,
                Timestamp timestamp, Order order, Page page) {
            return invoke("select", String.class, Timestamp.class, Order.class,
                    Page.class).with(ccl, timestamp, order, page);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(String ccl,
                Timestamp timestamp, Page page) {
            return invoke("select", String.class, Timestamp.class, Page.class)
                    .with(ccl, timestamp, page);
        }

        @Override
        public void set(String key, Object value, Collection<Long> records) {
            invoke("set", String.class, Object.class, Collection.class)
                    .with(key, value, records);
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
        public Map<Long, Map<String, Set<Long>>> trace(
                Collection<Long> records) {
            return invoke("trace", Collection.class).with(records);
        }

        @Override
        public Map<Long, Map<String, Set<Long>>> trace(Collection<Long> records,
                Timestamp timestamp) {
            return invoke("trace", Collection.class, Timestamp.class)
                    .with(records, timestamp);
        }

        @Override
        public Map<String, Set<Long>> trace(long record) {
            return invoke("trace", long.class).with(record);
        }

        @Override
        public Map<String, Set<Long>> trace(long record, Timestamp timestamp) {
            return invoke("trace", long.class, Timestamp.class).with(record,
                    timestamp);
        }

        @Override
        public boolean unlink(String key, long destination, long source) {
            return invoke("unlink", String.class, long.class, long.class)
                    .with(key, destination, source);
        }

        @Override
        public boolean verify(String key, Object value, long record) {
            return invoke("verify", String.class, Object.class, long.class)
                    .with(key, value, record);
        }

        @Override
        public boolean verify(String key, Object value, long record,
                Timestamp timestamp) {
            return invoke("review", String.class, Object.class, long.class,
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
            invoke("verifyOrSet", String.class, Object.class, long.class)
                    .with(key, value, record);
        }

        @Override
        protected Concourse copyConnection() {
            return new Client(clazz, invoke("copyConnection").with(), loader);
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
                        parameterTypes[i] = loader
                                .loadClass(packageBase + "Timestamp");
                    }
                    else if(parameterTypes[i] == Operator.class) {
                        parameterTypes[i] = loader
                                .loadClass(packageBase + "thrift.Operator");
                    }
                    else if(parameterTypes[i] == Criteria.class) {
                        parameterTypes[i] = loader
                                .loadClass(packageBase + "lang.Criteria");
                    }
                    else if(parameterTypes[i] == Page.class) {
                        parameterTypes[i] = loader
                                .loadClass(packageBase + "lang.paginate.Page");
                    }
                    else if(parameterTypes[i] == Order.class) {
                        parameterTypes[i] = loader
                                .loadClass(packageBase + "lang.sort.Order");
                    }
                    else {
                        continue;
                    }
                }
                return new MethodProxy(Reflection.getMethodUnboxed(clazz,
                        method, parameterTypes));
            }
            catch (Exception e) {
                throw CheckedExceptions.wrapAsRuntimeException(e);
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
                            Class<?> rclazz = loader
                                    .loadClass(packageBase + "lang.Criteria");
                            Object robj;
                            try {
                                Method rfactory = rclazz.getMethod("parse",
                                        String.class);
                                rfactory.setAccessible(true);
                                robj = rfactory.invoke(null, obj.ccl());
                            }
                            catch (NoSuchMethodException e) {
                                // In Concourse versions prior to 0.10, Criteria
                                // was a concrete class instead of an interface
                                // so we have to manually reconstruct the class
                                // and all of its symbols...
                                Field symbolField = Criteria.class
                                        .getDeclaredField("symbols");
                                symbolField.setAccessible(true);
                                List<Symbol> symbols = (List<Symbol>) symbolField
                                        .get(obj);
                                Constructor<?> rconstructor = rclazz
                                        .getDeclaredConstructor();
                                rconstructor.setAccessible(true);
                                robj = rconstructor.newInstance();
                                Method rmethod = rclazz.getDeclaredMethod("add",
                                        loader.loadClass(
                                                packageBase + "lang.Symbol"));
                                rmethod.setAccessible(true);
                                for (Symbol symbol : symbols) {
                                    Object rsymbol = null;
                                    if(symbol instanceof Enum) {
                                        rsymbol = loader
                                                .loadClass(symbol.getClass()
                                                        .getName())
                                                .getMethod("valueOf",
                                                        String.class)
                                                .invoke(null, ((Enum<?>) symbol)
                                                        .name());
                                    }
                                    else {
                                        Method symFactory = loader
                                                .loadClass(symbol.getClass()
                                                        .getName())
                                                .getMethod("parse",
                                                        String.class);
                                        symFactory.setAccessible(true);
                                        rsymbol = symFactory.invoke(null,
                                                symbol.toString());
                                    }
                                    rmethod.invoke(robj, rsymbol);
                                }
                            }
                            args[i] = robj;
                        }
                        else if(args[i] instanceof Order) {
                            Order order = (Order) args[i];
                            List<OrderComponent> spec = order.spec();
                            Class<?> rclazz = loader.loadClass(
                                    packageBase + "lang.sort.BuiltOrder");
                            Constructor<?> rconstructor = rclazz
                                    .getDeclaredConstructor();
                            rconstructor.setAccessible(true);
                            Object robj = rconstructor.newInstance();
                            Method rmethod = rclazz.getDeclaredMethod("set",
                                    String.class, long.class, int.class);
                            rmethod.setAccessible(true);
                            for (OrderComponent component : spec) {
                                rmethod.invoke(robj, component.key(),
                                        component.timestamp() == null ? -1
                                                : component.timestamp()
                                                        .getMicros(),
                                        component.direction().ordinal());
                            }
                            args[i] = robj;
                        }
                        else if(args[i] instanceof Page) {
                            Page obj = (Page) args[i];
                            Method offsetField = Page.class
                                    .getDeclaredMethod("offset");
                            offsetField.setAccessible(true);
                            int offset = (Integer) offsetField.invoke(obj);
                            Method limitField = Page.class
                                    .getDeclaredMethod("limit");
                            limitField.setAccessible(true);
                            int limit = (Integer) limitField.invoke(obj);
                            args[i] = loader
                                    .loadClass(
                                            packageBase + "lang.paginate.Page")
                                    .getMethod("of", int.class, int.class)
                                    .invoke(null, offset, limit);
                        }
                        else {
                            continue;
                        }
                    }
                    return (T) transformServerObject(
                            method.invoke(delegate, args));
                }
                catch (Exception e) {
                    throw CheckedExceptions.wrapAsRuntimeException(e);
                }
            }

            /**
             * If necessary, given an {@code object} returned from the managed
             * server, transform it to a class that comes from the test
             * application's classpath.
             * 
             * @param object the object from the managed server (usually a
             *            method's
             *            return value)
             * @return the transformed object
             * @throws ReflectiveOperationException
             */
            private Object transformServerObject(Object object)
                    throws ReflectiveOperationException {
                if(object == null) {
                    return object;
                }
                else if(object instanceof Set) {
                    Set<Object> transformed = Sets.newLinkedHashSet();
                    for (Object item : (Set<?>) object) {
                        transformed.add(transformServerObject(item));
                    }
                    object = transformed;
                }
                else if(object.getClass().getSimpleName()
                        .equals(Link.class.getSimpleName())) {
                    long longValue = (long) loader
                            .loadClass(packageBase + Link.class.getSimpleName())
                            .getMethod("longValue").invoke(object);
                    object = Link.to(longValue);
                }
                else if(object.getClass().getSimpleName()
                        .equals(Timestamp.class.getSimpleName())) {
                    long micros = (long) loader
                            .loadClass(packageBase
                                    + Timestamp.class.getSimpleName())
                            .getMethod("getMicros").invoke(object);
                    object = Timestamp.fromMicros(micros);
                }
                return object;
            }
        }
    }

    /**
     * The valid options for the {@link #clientConfigCleanupAction} variable.
     * 
     * @author Jeff Nelson
     */
    enum ClientConfigCleanupAction {
        DELETE, NONE, RESTORE_BACKUP
    }
}
