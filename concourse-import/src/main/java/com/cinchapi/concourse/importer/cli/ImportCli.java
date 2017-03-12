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
package com.cinchapi.concourse.importer.cli;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nullable;

import org.reflections.Reflections;

import jline.TerminalFactory;
import jline.console.ConsoleReader;

import com.beust.jcommander.Parameter;
import com.cinchapi.common.groovy.GroovyFiles;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.cli.CommandLineInterface;
import com.cinchapi.concourse.cli.Options;
import com.cinchapi.concourse.importer.CsvImporter;
import com.cinchapi.concourse.importer.Headered;
import com.cinchapi.concourse.importer.Importer;
import com.cinchapi.concourse.importer.JsonImporter;
import com.cinchapi.concourse.importer.LegacyCsvImporter;
import com.cinchapi.concourse.util.FileOps;
import com.cinchapi.concourse.util.Strings;
import com.google.common.base.CaseFormat;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;

/**
 * A CLI that uses the import framework to import data into Concourse.
 * 
 * @author Jeff Nelson
 */
@SuppressWarnings("deprecation")
public class ImportCli extends CommandLineInterface {

    /**
     * Aliases for built-in importer types. These aliases should be specified
     * using the {@code -t} or {@code --type} flag when invoking the CLI.
     */
    private static Map<String, Class<? extends Importer>> importers = Maps
            .newHashMapWithExpectedSize(3);
    static {
        // NOTE: Be sure to increase the expectedSize parameter for the map when
        // adding additional aliases
        importers.put("csv", CsvImporter.class);
        importers.put(".csv", LegacyCsvImporter.class); // deprecated
        importers.put("json", JsonImporter.class);
    }

    /*
     * TODO
     * 2) add flags to whitelist or blacklist files in a directory
     */

    /**
     * Construct a new instance.
     * 
     * @param args
     */
    public ImportCli(String[] args) {
        super(args);
    }

    @Override
    protected void doTask() {
        final ImportOptions opts = (ImportOptions) options;
        final Set<Long> records;
        final Constructor<? extends Importer> constructor = getConstructor(
                opts.type);
        opts.dynamic.put(Importer.ANNOTATE_DATA_SOURCE_OPTION_NAME,
                Boolean.toString(opts.annotateDataSource));
        if(opts.data == null) { // Import data from stdin
            Importer importer = Reflection.newInstance(constructor, concourse);
            if(!opts.dynamic.isEmpty()) {
                importer.setParams(options.dynamic);
            }
            if(importer instanceof Headered && !opts.header.isEmpty()) {
                ((Headered) importer).parseHeader(opts.header);
            }
            try {
                ConsoleReader reader = new ConsoleReader();
                String line;
                records = Sets.newLinkedHashSet();
                Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

                    // Interactive import is ended when user presses CTRL + C,
                    // so we need this shutdown hook to ensure that they get
                    // feedback about the import before the JVM dies.

                    @Override
                    public void run() {
                        if(options.verbose) {
                            System.out.println(records);
                        }
                        System.out.println(
                                Strings.format("Imported data into {} records",
                                        records.size()));
                    }

                }));
                try {
                    final AtomicBoolean lock = new AtomicBoolean(false);
                    new Thread(new Runnable() { // If there is no input in
                                                // 100ms, assume that the
                                                // session is interactive (i.e.
                                                // not piped) and display a
                                                // prompt

                        @Override
                        public void run() {
                            try {
                                Thread.sleep(100);
                                if(lock.compareAndSet(false, true)) {
                                    System.out.println(
                                            "Importing from stdin. Press "
                                                    + "CTRL + C when finished");
                                }
                            }
                            catch (InterruptedException e) {}
                        }

                    }).start();
                    while ((line = reader.readLine()) != null) {
                        try {
                            lock.set(true);
                            records.addAll(importer.importString(line));
                        }
                        catch (Exception e) {
                            System.err.println(e);
                        }
                    }
                }
                catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
            finally {
                try {
                    TerminalFactory.get().restore();
                }
                catch (Exception e) {
                    throw Throwables.propagate(e);
                }
            }
        }
        else {
            String path = FileOps.expandPath(opts.data, getLaunchDirectory());
            Collection<String> files = FileOps.isDirectory(path)
                    ? scan(Paths.get(path)) : ImmutableList.of(path);
            Stopwatch watch = Stopwatch.createUnstarted();
            if(files.size() > 1) {
                records = Sets.newConcurrentHashSet();
                final Queue<String> filesQueue = (Queue<String>) files;
                List<Runnable> runnables = Lists
                        .newArrayListWithCapacity(opts.numThreads);
                // Create just enough Runnables with instantiated Importers in
                // advance. Each of those Runnables will work until #filesQueue
                // is exhausted.
                opts.numThreads = Math.min(opts.numThreads, files.size());
                for (int i = 0; i < opts.numThreads; ++i) {
                    final Importer importer0 = Reflection.newInstance(
                            constructor,
                            i == 0 ? concourse
                                    : Concourse.connect(opts.host, opts.port,
                                            opts.username, opts.password,
                                            opts.environment));
                    if(!opts.dynamic.isEmpty()) {
                        importer0.setParams(opts.dynamic);
                    }
                    if(importer0 instanceof Headered
                            && !opts.header.isEmpty()) {
                        ((Headered) importer0).parseHeader(opts.header);
                    }
                    runnables.add(new Runnable() {

                        private final Importer importer = importer0;

                        @Override
                        public void run() {
                            String file;
                            while ((file = filesQueue.poll()) != null) {
                                records.addAll(importer.importFile(file));
                            }
                        }

                    });
                }
                ExecutorService executor = Executors
                        .newFixedThreadPool(runnables.size());
                System.out.println("Starting import...");
                watch.start();
                for (Runnable runnable : runnables) {
                    executor.execute(runnable);
                }
                executor.shutdown();
                try {
                    if(!executor.awaitTermination(1, TimeUnit.MINUTES)) {
                        while (!executor.isTerminated()) {
                            System.out.print('.'); // block until all tasks are
                                                   // completed and provide some
                                                   // feedback to the user
                        }
                    }
                }
                catch (InterruptedException e) {
                    throw Throwables.propagate(e);
                }
            }
            else {
                Importer importer = Reflection.newInstance(constructor,
                        concourse);
                if(!opts.dynamic.isEmpty()) {
                    importer.setParams(opts.dynamic);
                }
                if(importer instanceof Headered && !opts.header.isEmpty()) {
                    ((Headered) importer).parseHeader(opts.header);
                }
                System.out.println("Starting import...");
                watch.start();
                records = importer.importFile(files.iterator().next());
            }
            watch.stop();
            long elapsed = watch.elapsed(TimeUnit.MILLISECONDS);
            double seconds = elapsed / 1000.0;
            if(options.verbose) {
                System.out.println(records);
            }
            System.out.println(MessageFormat.format(
                    "Imported data " + "into {0} records in {1} seconds",
                    records.size(), seconds));
        }
    }

    @Override
    protected Options getOptions() {
        return new ImportOptions();
    }

    /**
     * Return the host Concourse Server's "home" directory.
     * 
     * @return the host application's home
     */
    @Nullable
    private static Path getConcourseServerHome() {
        // NOTE: This is a HACK! This method is borrowed from Concourse Server's
        // ManagementCli interface because we know that the provided ImportCli
        // will likely live in a Concourse Deployment's standard directory from
        // CLIs.
        String path = System.getProperty("user.app.home"); // this is set by the
                                                           // .env script that
                                                           // is sourced by
                                                           // every server-side
                                                           // CLI
        return path != null ? Paths.get(path) : null;
    }

    /**
     * Return the appropriate constructor for creating instances of the
     * {@code type} {@link Importer}.
     * 
     * <p>
     * To make the {@link Importer}, call
     * {@link Constructor#newInstance(Object...)} with a non-shared Concourse
     * connection.
     * </p>
     * 
     * @param type the {@link #importers alias} or fully qualified name for the
     *            desired {@link Importer} class
     * @return the constructor
     */
    private static Constructor<? extends Importer> getConstructor(String type) {
        Class<? extends Importer> clz = importers.get(type);
        if(clz == null) {
            try {
                clz = getCustomImporterClass(type);
            }
            catch (ClassNotFoundException e) {
                throw new RuntimeException(Strings
                        .format("{} is not a valid importer type.", type));
            }
        }
        try {
            return clz.getDeclaredConstructor(Concourse.class);
        }
        catch (NoSuchMethodException e) {
            // This should never happen because Importer base class mandates the
            // existence of the constructor in the subclass by not defining a
            // no-arg alternative
            throw Throwables.propagate(e);
        }
    }

    /**
     * Given an alias (or fully qualified class name) attempt to load a "custom"
     * importer that is not already defined in the {@link #importers built-in}
     * collection.
     * 
     * @param alias a conventional alias (i.e. FileTypeImporter --> file-type)
     *            OR a fully qualified class name OR the path to a customer
     *            importer file (can be .groovy or .jar) OR the name of a
     *            customer importer file contained in the "importers" directory
     *            of the server's home
     * @return the {@link Class} that corresponds to the custom importer
     * @throws ClassNotFoundException
     */
    @SuppressWarnings("unchecked")
    private static Class<? extends Importer> getCustomImporterClass(
            String alias) throws ClassNotFoundException {
        try {
            return (Class<? extends Importer>) Class.forName(alias);
        }
        catch (ClassNotFoundException e) {
            Path path;
            boolean exists = true;
            if(!(path = Paths.get(alias)).toFile().exists()) {
                exists = false;
                Path concourseServerHome = getConcourseServerHome();
                if(concourseServerHome != null) {
                    Path importers = getConcourseServerHome()
                            .resolve("importers");
                    String[] candidates = { alias, alias + ".groovy",
                            alias + ".jar" };
                    for (String candidate : candidates) {
                        if((path = importers.resolve(candidate)).toFile()
                                .exists()) {
                            exists = true;
                            break;
                        }
                    }
                }
            }
            if(exists) {
                if(path.toString().endsWith(".groovy")) {
                    return GroovyFiles.loadClass(path);
                }
                else {
                    throw new UnsupportedOperationException(
                            "Cannot define custom importer in a .jar file");
                }
            }
            else {
                // Attempt to determine the correct class name from the alias by
                // loading the server's classpath. For the record, this is hella
                // slow.
                Reflections.log = null; // turn off reflection logging
                Reflections reflections = new Reflections();
                char firstChar = alias.charAt(0);
                for (Class<? extends Importer> clazz : reflections
                        .getSubTypesOf(Importer.class)) {
                    String name = clazz.getSimpleName();
                    if(name.length() == 0) { // Skip anonymous subclasses
                        continue;
                    }
                    char nameFirstChar = name.charAt(0);
                    if(!Modifier.isAbstract(clazz.getModifiers())
                            && (nameFirstChar == Character
                                    .toUpperCase(firstChar)
                                    || nameFirstChar == Character
                                            .toLowerCase(firstChar))) {
                        String expected = CaseFormat.UPPER_CAMEL
                                .to(CaseFormat.LOWER_HYPHEN,
                                        clazz.getSimpleName())
                                .replaceAll("-importer", "");
                        if(alias.equals(expected)) {
                            return clazz;
                        }
                    }
                }
            }
            throw e;

        }
    }

    /**
     * Recursively scan and collect all the files in the directory defined by
     * {@code path}.
     * 
     * @param path a {@link Path} that is already verified to
     *            {@link java.nio.file.Files#isDirectory(Path, java.nio.file.LinkOption...)
     *            to be a directory}.
     * @return the collection of files in the directory
     */
    private static Queue<String> scan(Path path) {
        try (DirectoryStream<Path> stream = java.nio.file.Files
                .newDirectoryStream(path)) {
            Iterator<Path> it = stream.iterator();
            Queue<String> files = Queues.newConcurrentLinkedQueue();
            while (it.hasNext()) {
                Path thePath = it.next();
                if(java.nio.file.Files.isDirectory(thePath)) {
                    files.addAll(scan(thePath));
                }
                else {
                    files.add(thePath.toString());
                }
            }
            return files;
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Import specific {@link Options}.
     * 
     * @author jnelson
     */
    protected static class ImportOptions extends Options {

        @Parameter(names = { "-d",
                "--data" }, description = "The path to the file or directory to import; if no source is provided read from stdin")
        public String data;

        @Parameter(names = "--numThreads", description = "The number of worker threads to use for a multithreaded import")
        public int numThreads = Runtime.getRuntime().availableProcessors();

        @Parameter(names = { "-r",
                "--resolveKey" }, description = "The key to use when resolving data into existing records")
        public String resolveKey = null;

        @Parameter(names = { "-t",
                "--type" }, description = "The name/type of the importer to use")
        public String type = "csv";

        @Parameter(names = "--header", description = "A custom header to assign for supporting importers")
        public String header = "";

        @Parameter(names = "--annotate-data-source", description = "Add the filename from which the data is imported as a value for the '__datasource' key on every imported object")
        public boolean annotateDataSource = false;

    }

}
