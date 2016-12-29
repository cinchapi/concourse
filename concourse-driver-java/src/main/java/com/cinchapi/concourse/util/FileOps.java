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
package com.cinchapi.concourse.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.Watchable;
import java.util.AbstractList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.imca_cat.pollingwatchservice.PollingWatchService;

import com.cinchapi.common.base.CheckedExceptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.sun.nio.file.SensitivityWatchEventModifier;

/**
 * Generic file utility methods that compliment and expand upon those found in
 * {@link java.nio.file.Files} and {@link com.google.common.io.Files}.
 * 
 * @author Jeff Nelson
 */
public class FileOps {

    /**
     * Write the String {@code content} to the end of the {@code file},
     * preserving anything that was previously there.
     * 
     * @param content the data to write
     * @param file the path to the file
     */
    public static void append(String content, String file) {
        try {
            Files.append(content, new File(file), StandardCharsets.UTF_8);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Cause the current thread to block while waiting for a change to
     * {@code file}.
     * 
     * @param file the path to a regular file
     */
    public static void awaitChange(String file) {
        Path path = Paths.get(expandPath(file));
        Preconditions.checkArgument(java.nio.file.Files.isRegularFile(path));
        WatchEvent.Kind<?>[] kinds = { StandardWatchEventKinds.ENTRY_MODIFY };
        SensitivityWatchEventModifier[] modifiers = {
                SensitivityWatchEventModifier.HIGH };
        Watchable parent = path.getParent().toAbsolutePath();
        if(!REGISTERED_WATCHER_PATHS.contains(parent)) {
            for (int i = 0; i < FILE_CHANGE_WATCHERS.size(); ++i) {
                WatchService watcher = FILE_CHANGE_WATCHERS.get(i);
                try {
                    if(watcher instanceof PollingWatchService) {
                        ((PollingWatchService) watcher).register((Path) parent,
                                kinds, modifiers);
                    }
                    else {
                        parent.register(watcher, kinds, modifiers);
                    }
                    break;
                }
                catch (IOException e) {
                    // If an error occurs while trying to register a path with a
                    // watch service, cycle through the list in order to see if
                    // we can find one that will accept it.
                    if(i < FILE_CHANGE_WATCHERS.size()) {
                        continue;
                    }
                    else {
                        throw CheckedExceptions.throwAsRuntimeException(e);
                    }
                }
            }
            REGISTERED_WATCHER_PATHS.add(parent);
        }
        String sync = path.toString().intern();
        try {
            synchronized (sync) {
                sync.wait();
            }
        }
        catch (InterruptedException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Expand the given {@code path} so that it contains completely normalized
     * components (e.g. ".", "..", and "~" are resolved to the correct absolute
     * paths).
     * 
     * @param path
     * @return the expanded path
     */
    public static String expandPath(String path) {
        return expandPath(path, null);
    }

    /**
     * Expand the given {@code path} so that it contains completely normalized
     * components (e.g. ".", "..", and "~" are resolved to the correct absolute
     * paths).
     * 
     * @param path
     * @param cwd
     * @return the expanded path
     */
    public static String expandPath(String path, String cwd) {
        path = path.replaceAll("~", USER_HOME);
        Path base = com.google.common.base.Strings.isNullOrEmpty(cwd)
                ? BASE_PATH : FileSystems.getDefault().getPath(cwd);
        return base.resolve(path).normalize().toString();
    }

    /**
     * Return the home directory of the user of the parent process for this JVM.
     * 
     * @return the home directory
     */
    public static String getUserHome() {
        return USER_HOME;
    }

    /**
     * Get the working directory of this JVM, which is the directory from which
     * the process is launched.
     * 
     * @return the working directory
     */
    public static String getWorkingDirectory() {
        return WORKING_DIRECTORY;
    }

    /**
     * Return {@code true} if the specified {@code path} is that of a directory
     * and not a flat file.
     * 
     * @param path the path to check
     * @return {@code true} if the {@code path} is that of a directory
     */
    public static boolean isDirectory(String path) {
        return java.nio.file.Files.isDirectory(Paths.get(path));
    }

    /**
     * Create the directories named by {@code path}, including any necessary,
     * but nonexistent parent directories.
     * <p>
     * <strong>NOTE:</strong> If this operation fails, it may have succeeded in
     * creating some of the necessary parent directories.
     * </p>
     * 
     * @param path the path of directories to create
     * @return {@code true} if entire {@code path} was created
     */
    public static void mkdirs(String path) {
        try {
            java.nio.file.Files.createDirectories(Paths.get(path));
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Read the contents of {@code file} into a UTF-8 string.
     * 
     * @param file
     * @return the file content
     */
    public static String read(String file) {
        try {
            return com.google.common.io.Files.toString(new File(file),
                    StandardCharsets.UTF_8);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Return a list that lazily accumulates lines in the underlying
     * {@code file}.
     * <p>
     * This method is really just syntactic sugar for reading lines from a file,
     * so the returned list doesn't actually allow any operations other than
     * forward iteration.
     * </p>
     * 
     * @param file
     * @return a "list" of lines in the file
     */
    public static List<String> readLines(final String file) {
        return readLines(file, null);
    }

    /**
     * Return a list that lazily accumulates lines in the underlying
     * {@code file}.
     * <p>
     * This method is really just syntactic sugar for reading lines from a file,
     * so the returned list doesn't actually allow any operations other than
     * forward iteration.
     * </p>
     * 
     * @param file
     * @param cwd
     * @return a "list" of lines in the file
     */
    public static List<String> readLines(final String file, String cwd) {
        final String rwd = MoreObjects.firstNonNull(cwd, WORKING_DIRECTORY);
        return new AbstractList<String>() {

            @Override
            public String get(int index) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Iterator<String> iterator() {
                return new ReadOnlyIterator<String>() {

                    String line = null;
                    BufferedReader reader;
                    {
                        try {
                            reader = new BufferedReader(new FileReader(
                                    FileOps.expandPath(file, rwd)));
                            line = reader.readLine();
                        }
                        catch (IOException e) {
                            throw Throwables.propagate(e);
                        }
                    }

                    @Override
                    public boolean hasNext() {
                        return this.line != null;
                    }

                    @Override
                    public String next() {
                        String result = line;
                        try {
                            line = reader.readLine();
                            if(line == null) {
                                reader.close();
                            }
                            return result;
                        }
                        catch (IOException e) {
                            throw Throwables.propagate(e);
                        }
                    }

                };
            }

            @Override
            public int size() {
                int size = 0;
                Iterator<String> it = iterator();
                while (it.hasNext()) {
                    size += 1;
                    it.next();
                }
                return size;
            }

        };
    }

    /**
     * Create a temporary directory with the specified {@code prefix}.
     * 
     * @param prefix the directory name prefix
     * @return the path to the temporary directory
     */
    public static String tempDir(String prefix) {
        try {
            return java.nio.file.Files.createTempDirectory(prefix).toString();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Create a temporary file that is likely to be deleted some time after this
     * JVM terminates, but definitely not before.
     * 
     * @return the absolute path where the temp file is stored
     */
    public static String tempFile() {
        return tempFile("cnch", null);
    }

    /**
     * Create a temporary file that is likely to be deleted some time after this
     * JVM terminates, but definitely not before.
     * 
     * @param prefix the prefix for the temp file
     * @return the absolute path where the temp file is stored
     */
    public static String tempFile(String prefix) {
        return tempFile(prefix, null);
    }

    /**
     * Create a temporary file that is likely to be deleted some time after this
     * JVM terminates, but definitely not before.
     * 
     * @param prefix the prefix for the temp file
     * @param suffix the suffix for the temp file
     * @return the absolute path where the temp file is stored
     */
    public static String tempFile(String prefix, String suffix) {
        return tempFile(null, prefix, suffix);
    }

    /**
     * Create a temporary file that is likely to be deleted some time after this
     * JVM terminates, but definitely not before.
     * 
     * @param dir the directory in which the temp file should be created
     * @param prefix the prefix for the temp file
     * @param suffix the suffix for the temp file
     * @return the absolute path where the temp file is stored
     */
    public static String tempFile(String dir, String prefix, String suffix) {
        prefix = prefix == null ? "cnch" : prefix;
        prefix = prefix.trim();
        while (prefix.length() < 3) { // java enforces prefixes of >= 3
                                      // characters
            prefix = prefix + Random.getString().charAt(0);
        }
        try {
            if(dir != null) {
                FileOps.mkdirs(dir);
            }
            return dir == null
                    ? java.nio.file.Files.createTempFile(prefix, suffix)
                            .toAbsolutePath().toString()
                    : java.nio.file.Files
                            .createTempFile(Paths.get(dir), prefix, suffix)
                            .toAbsolutePath().toString();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Create an empty file or update the last updated timestamp on the same as
     * the unix command of the same name.
     * 
     * @param file the path of the file to touch
     * @return the value of {@code file} in case it needs to be passed to a
     *         super constructor
     */
    public static String touch(String file) {
        try {
            Files.touch(new File(file));
            return file;
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * A shortcut for getting a {@link URL} instance from a file path.
     * 
     * @param path the path to the file or directory
     * @return the {@link URL} that corresponds to {@code path}
     */
    public static URL toURL(String path) {
        try {
            return new File(path).toURI().toURL();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Write the String {@code content} to the {@code file}, overwriting
     * anything that was previously there.
     * 
     * @param content the data to write
     * @param file the path to the file
     */
    public static void write(String content, String file) {
        try {
            Files.write(content, new File(file), StandardCharsets.UTF_8);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Configure the watch {@code service} to notify listeners about changes to
     * any of the {@link #REGISTERED_WATCHER_PATHS} that have been sent to the
     * service.
     * 
     * @param service the {@link WatchService} to setup
     */
    private static void setupWatchService(WatchService service) {
        Thread t = new Thread(() -> {
            try {
                while (true) {
                    WatchKey key;
                    try {
                        key = service.take();
                        for (WatchEvent<?> event : key.pollEvents()) {
                            Path parent = (Path) key.watchable();
                            WatchEvent.Kind<?> kind = event.kind();
                            if(kind == StandardWatchEventKinds.ENTRY_MODIFY) {
                                Path abspath = parent
                                        .resolve((Path) event.context())
                                        .toAbsolutePath();
                                String sync = abspath.toString().intern();
                                synchronized (sync) {
                                    sync.notifyAll();
                                }
                            }
                        }
                        key.reset();
                    }
                    catch (InterruptedException e) {
                        throw Throwables.propagate(e);
                    }
                }
            }
            finally {
                try {
                    service.close();
                }
                catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            }

        });
        t.setDaemon(true);
        t.start();
    }

    /**
     * A service that watches directories for operations on files.
     * <p>
     * Java's {@link WatchService} API is designed to handle directories instead
     * of individual files. So, when {@link #awaitChange(String)} is called, we
     * register the parent path (e.g. the housing directory) with the watch
     * service and check the {@link WatchEvent watch event's}
     * {@link WatchEvent#context() context} to determine whether an individual
     * file has changed.
     * </p>
     */
    private static List<WatchService> FILE_CHANGE_WATCHERS = Lists
            .newArrayListWithCapacity(2);
    static {
        try {
            // Add a PollingWatchService to use as a backup in case the default
            // watch service is causing issues (i.e. on Linux the max number of
            // inotify watches may be reached, in which case we can use the
            // backup as a fail safe.)
            PollingWatchService pollingWatchService = new PollingWatchService(
                    Runtime.getRuntime().availableProcessors(), 500,
                    TimeUnit.MILLISECONDS);
            pollingWatchService.start();
            FILE_CHANGE_WATCHERS.add(pollingWatchService);
            if(!Platform.isLinux()) {
                // NOTE: Seems like there are problems when inotify actually
                // working on Linux, so for now, don't set it as a possible
                // watch service...
                FILE_CHANGE_WATCHERS
                        .add(FileSystems.getDefault().newWatchService());
            }
        }
        catch (Exception e) {
            // NOTE: Cannot re-throw the exception because it will prevent the
            // class from being loaded...
            e.printStackTrace();
        }
        FILE_CHANGE_WATCHERS.forEach((watcher) -> {
            setupWatchService(watcher);
        });
    }

    /**
     * A collection of {@link Watchable} paths that have already been registered
     * with the {@link #FILE_CHANGE_WATCHER}.
     */
    @VisibleForTesting
    protected static Set<Watchable> REGISTERED_WATCHER_PATHS = Sets
            .newConcurrentHashSet();

    /**
     * The user's home directory, which is used to expand path names with "~"
     * (tilde).
     */
    private static String USER_HOME = System.getProperty("user.home");

    /**
     * The working directory from which the current JVM process was launched.
     */
    private static String WORKING_DIRECTORY = System.getProperty("user.dir");

    /**
     * The base path that is used to resolve and normalize other relative paths.
     */
    private static Path BASE_PATH = FileSystems.getDefault()
            .getPath(WORKING_DIRECTORY);

    protected FileOps() {/* noop */}

}
