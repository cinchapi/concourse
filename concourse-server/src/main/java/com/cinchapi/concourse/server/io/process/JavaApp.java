/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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
package com.cinchapi.concourse.server.io.process;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.tools.JavaCompiler;
import javax.tools.JavaCompiler.CompilationTask;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.StandardLocation;
import javax.tools.ToolProvider;

import com.cinchapi.common.process.ProcessTerminationListener;
import com.cinchapi.common.process.ProcessWatcher;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.util.Platform;
import com.cinchapi.concourse.util.Processes;
import com.cinchapi.concourse.util.TLists;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

/**
 * An {@link JavaApp} takes a dynamic set of instructions, written as a formal
 * Java class with a main method, and runs them in a separate {@link Process}.
 *
 * @author Jeff Nelson
 */
public class JavaApp extends Process {

    /**
     * Return the injected source code that goes into the main method of the
     * JavaApp's source to help detect when the host process has terminated.
     * 
     * @return the injected source code
     */
    private static String getHostWatcherCodeInjectionMainBlock() {
        // @formatter:off
        return ""
        + "      try {\n"
        + "        watchThreadStarted.await();\n"
        + "      }\n"
        + "      catch(InterruptedException e) {\n"
        + "        throw new RuntimeException(e);\n"
        + "      }\n";
        // @formatter:on
    }

    /**
     * Return the injected source code that goes into a static block immediately
     * after the main class declaration to help detect when the host process has
     * terminated.
     * 
     * @param pid the pid of the host process to watch for termination
     * @return the injected source code
     */
    private static String getHostWatcherCodeInjectionStaticBlock(String pid) {
        // @formatter:off
        return ""
        + "static java.util.concurrent.CountDownLatch watchThreadStarted = new java.util.concurrent.CountDownLatch(1);\n"
        + "static {\n"
        + "  Thread main = Thread.currentThread();"
        + "  Thread t = new Thread(new Runnable() {\n"
        + "    @Override\n"
        + "    public void run() {\n"
        + "      "+ProcessWatcher.class.getName()+" watcher = new "+ProcessWatcher.class.getName()+"();\n"
        + "      java.util.concurrent.atomic.AtomicBoolean terminated = new java.util.concurrent.atomic.AtomicBoolean(false);\n"
        + "      watcher.watch(\""+pid+"\", new "+ProcessTerminationListener.class.getName()+"() {\n"
        + "        @Override\n"
        + "        public void onTermination() {\n"
        + "          terminated.set(true);\n"
        + "        }\n"
        + "      });\n"
        + "        watchThreadStarted.countDown();\n"
        + "        while(!terminated.get() && main.isAlive()) {\n"
        + "          Thread.yield();\n"
        + "          continue;\n"
        + "        }"
        + "      System.exit(0);\n"
        + "    }\n"
        + "  });\n"
        + "  t.setDaemon(true);\n"
        + "  t.start();\n"
        + "}";
        // @formatter:on
    }

    /**
     * Given the input {@code source}, inject the code necessary to watch the
     * host process for termination.
     * 
     * @param source the input source
     * @return the source after additional code has been injected
     */
    private static String injectHostWatcherCode(String source) {
        String pid = Processes.getCurrentPid();
        StringBuilder builder = new StringBuilder(source);
        // First, inject a static block of code right after the class
        // declaration
        int index = builder.indexOf("{");
        builder.insert(index + 1,
                "\n" + getHostWatcherCodeInjectionStaticBlock(pid));
        // Inject a block of code at the beginning of the main method to use a
        // ProcessWatcher to detect when the host process terminates
        index = builder.indexOf("{", builder.indexOf("static void main"));
        builder.insert(index + 1, System.lineSeparator()
                + getHostWatcherCodeInjectionMainBlock());
        return builder.toString();
    }

    /**
     * Inject the necessary jars onto the input classpath to ensure that the
     * host watcher code can compile.
     * 
     * @param classpath the input classpath
     * @return the updated classpath with additional jars
     */
    private static String injectHostWatcherJarsOntoClasspath(String classpath) {
        final File f = new File(ProcessWatcher.class.getProtectionDomain()
                .getCodeSource().getLocation().getPath());
        return classpath += CLASSPATH_SEPARATOR + f.getAbsolutePath();
    }

    /**
     * Make sure that the {@code source} has all the necessary components and
     * return the name of the main class.
     *
     * @param source the source code
     * @return the name of the main class
     */
    private static String validateSource(String source) {
        Preconditions.checkArgument(source.contains("static void main(String"),
                "Source must contain a main method");
        String[] toks = source.split("public class");
        Preconditions.checkArgument(toks.length >= 2,
                "Source must define a top-level public class");
        String clazz = toks[1].trim().split(" ")[0].trim();
        return clazz;
    }

    /**
     * The separator character to use between different elements on the
     * classpath.
     */
    public static final String CLASSPATH_SEPARATOR = Platform.isWindows() ? ";"
            : ":";

    /**
     * The amount of time between each check to determine if the app shutdown
     * prematurely.
     */
    @VisibleForTesting
    protected static int PREMATURE_SHUTDOWN_CHECK_INTERVAL_IN_MILLIS = 3000;

    /**
     * The classpath to use when launching the external JVM process.
     */
    private final String classpath;

    /**
     * A flag that indicates whether the app has been compiled.
     */
    private boolean compiled = false;

    /**
     * A reflective reference to a field that my container information about
     * whether {@link #process} has exited.
     */
    private Field hasExited = null;

    /**
     * The absolute path to the java binary that is used to launch the JVM.
     */
    private final String javaBinary;

    /**
     * The name of the top-level class that contains the main method. This is
     * extracted from the source in the {@link #validateSource(String)} method.
     */
    private final String mainClass;

    /**
     * Options to pass to the JVM.
     */
    private final String[] options;

    /**
     * The underlying process that controls the app.
     */
    private Process process;

    /**
     * The system dependent separator.
     */
    private final String separator;

    /**
     * The absolute path to the location where the source is stored on disk.
     */
    private final String sourceFile;

    /**
     * A scheduled executor to watch for premature shutdowns, if
     * {@link #onPrematureShutdown(PrematureShutdownHandler)} is configured.
     */
    private ScheduledExecutorService watcher;

    /**
     * The temporary directory where the source is saved and compiled and the
     * java process is launched.
     */
    private final String workspace;

    /**
     * Construct a new instance.
     *
     * @param source
     */
    public JavaApp(String source) {
        this(System.getProperty("java.class.path"), source);
    }

    /**
     * Construct a new instance.
     *
     * @param classpath
     * @param source
     * @param options
     */
    public JavaApp(String classpath, String source, ArrayList<String> options) {
        this(classpath, source,
                (String[]) options.toArray(new String[options.size()]));
    }

    /**
     * Construct a new instance.
     *
     * @param classpath
     * @param source
     * @param options
     */
    public JavaApp(String classpath, String source, String... options) {
        source = injectHostWatcherCode(source);
        classpath = injectHostWatcherJarsOntoClasspath(classpath);
        this.mainClass = validateSource(source);
        this.classpath = classpath;
        this.separator = System.getProperty("file.separator");
        this.javaBinary = System.getProperty("java.home") + separator + "bin"
                + separator + "java";
        this.workspace = Files.createTempDir().getAbsolutePath();
        this.options = options;

        // Save the source to a temporary file
        this.sourceFile = workspace + separator + mainClass + ".java";
        FileSystem.write(source, sourceFile);

        // Add Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

            @Override
            public void run() {
                JavaApp.this.destroy();
            }

        }));
    }

    /**
     * Attempt to compile the app and throw an {@link Exception} if an error
     * occurs.
     */
    public void compile() {
        if(!compiled) {
            JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
            int exit;
            if(classpath.length() > 0) {
                StandardJavaFileManager fileManager = compiler
                        .getStandardFileManager(null, null, null);
                List<File> cpList = Lists.newArrayList();
                String[] parts = classpath.split(CLASSPATH_SEPARATOR);
                for (String part : parts) {
                    cpList.add(new File(part));
                }
                try {
                    fileManager.setLocation(StandardLocation.CLASS_PATH,
                            cpList);
                }
                catch (IOException e) {
                    throw Throwables.propagate(e);
                }
                Iterable<? extends JavaFileObject> compilationUnits = fileManager
                        .getJavaFileObjectsFromStrings(
                                Arrays.asList(sourceFile));
                CompilationTask task = compiler.getTask(null, fileManager, null,
                        Lists.newArrayList("-classpath", classpath), null,
                        compilationUnits);
                exit = task.call() ? 0 : 1;
            }
            else {
                exit = compiler.run(null, null, null, sourceFile);
            }
            if(exit == 0) {
                compiled = true;
            }
            else {
                throw new RuntimeException("Could not compile source");
            }
        }
    }

    @Override
    public void destroy() {
        if(watcher != null) {
            watcher.shutdownNow();
        }
        if(process != null) {
            process.destroy();
        }
    }

    @Override
    public int exitValue() {
        return process.exitValue();
    }

    @Override
    public InputStream getErrorStream() {
        return process.getErrorStream();
    }

    @Override
    public InputStream getInputStream() {
        return process.getInputStream();
    }

    @Override
    public OutputStream getOutputStream() {
        return process.getOutputStream();
    }

    /**
     * Return {@code true} if the app is running.
     *
     * @return {@code true} of the app is running
     */
    public boolean isRunning() {
        if(process != null) {
            if(hasExited != null) {
                try {
                    return !hasExited.getBoolean(process);
                }
                catch (ReflectiveOperationException e) {
                    throw Throwables.propagate(e);
                }
            }
            else {
                try {
                    process.exitValue();
                    return false;
                }
                catch (IllegalThreadStateException e) {
                    // Hate using an exception for control flow, but Java gives
                    // us no choice in the matter here :-/
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Submit a {@link PrematureShutdownHandler task} to be executed whenever
     * this process prematurely shuts down.
     *
     * @param handler the task to execute
     */
    public void onPrematureShutdown(final PrematureShutdownHandler handler) {
        watcher = Executors.newSingleThreadScheduledExecutor();
        watcher.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                if(!isRunning()) {
                    handler.run(process.getInputStream(),
                            process.getErrorStream());
                    destroy();
                }
            }

        }, PREMATURE_SHUTDOWN_CHECK_INTERVAL_IN_MILLIS,
                PREMATURE_SHUTDOWN_CHECK_INTERVAL_IN_MILLIS,
                TimeUnit.MILLISECONDS);
    }

    /**
     * Run the app. Use the standard {@link Process} methods to do things like
     * {@link #waitFor() waiting for} the app the finish, getting the
     * {@link #exitValue() exit code} and getting the contents of the
     * {@link #getInputStream() output} and {@link #getErrorStream() error}
     * streams.
     */
    public void run() {
        compile();
        List<String> args = Lists.newArrayList(javaBinary, "-cp",
                classpath + ":.");
        for (String option : options) {
            args.add(option);
        }
        args.add(mainClass);
        ProcessBuilder builder = new ProcessBuilder(
                TLists.toArrayCasted(args, String.class));
        builder.directory(new File(workspace));
        try {
            process = builder.start();
            try {
                hasExited = process.getClass().getDeclaredField("hasExited");
                hasExited.setAccessible(true);
            }
            catch (ReflectiveOperationException e) {}
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Attempt to compile the app. Return {@code true} if successful and
     * {@code false} otherwise.
     *
     * @return {@code true} if the app successfully compiles
     */
    public boolean tryCompile() {
        try {
            compile();
            return true;
        }
        catch (RuntimeException e) {
            return false;
        }
    }

    @Override
    public int waitFor() throws InterruptedException {
        return process.waitFor();
    }

}
