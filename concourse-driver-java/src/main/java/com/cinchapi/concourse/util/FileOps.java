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
import java.util.AbstractList;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.MoreObjects;
import com.google.common.base.Throwables;
import com.google.common.io.Files;

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
        Path base = com.google.common.base.Strings.isNullOrEmpty(cwd) ? BASE_PATH
                : FileSystems.getDefault().getPath(cwd);
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

    protected FileOps() {/* noop */}

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
    private static Path BASE_PATH = FileSystems.getDefault().getPath(
            WORKING_DIRECTORY);

}
