/*
 * Copyright (c) 2013-2015 Cinchapi, Inc.
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
package org.cinchapi.concourse.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.AbstractList;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Throwables;

/**
 * File utilities that are used in the import classes.
 * 
 * @author Jeff Nelson
 */
public class FileOps {

    /**
     * Expand the given {@code path} so that it contains completely normalized
     * components (e.g. ".", "..", and "~" are resolved to the correct absolute
     * paths).
     * 
     * @param path
     * @return the expanded path
     */
    public static String expandPath(String path) {
        path = path.replaceAll("~", USER_HOME);
        return BASE_PATH.resolve(path).normalize().toString();
    }
    
    /**
     * Return the home directory of the parent process for this JVM.
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
        return new AbstractList<String>() {

            @Override
            public String get(int index) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Iterator<String> iterator() {
                return new Iterator<String>() {

                    String line = null;
                    BufferedReader reader;
                    {
                        try {
                            reader = new BufferedReader(new FileReader(
                                    FileOps.expandPath(file)));
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

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
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
