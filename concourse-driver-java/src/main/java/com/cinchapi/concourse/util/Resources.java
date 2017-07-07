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
package com.cinchapi.concourse.util;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Map;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;

/**
 * Utilities to handle getting resources in a standard and portable way.
 * 
 * @author Jeff Nelson
 */
public class Resources {

    /**
     * Collection which maps a resource name with its URL.
     */
    private static Map<String, URL> map = Maps.newHashMap();

    /**
     * Finds a resource with a given name. The rules for searching resources
     * associated with a given class are implemented by the defining
     * {@linkplain ClassLoader class loader} of the class. This method
     * delegates to this object's class loader. If this object was loaded by
     * the bootstrap class loader, the method delegates to
     * {@link ClassLoader#getSystemResourceAsStream}.
     * 
     * <p>
     * Before delegation, an absolute resource name is constructed from the
     * given resource name using this algorithm:
     * 
     * <ul>
     * 
     * <li>If the {@code name} begins with a {@code '/'} (<tt>'&#92;u002f'</tt>
     * ), then the absolute name of the resource is the portion of the
     * {@code name} following the {@code '/'}.
     * 
     * <li>Otherwise, the absolute name is of the following form:
     * 
     * <blockquote> {@code modified_package_name/name} </blockquote>
     * 
     * <p>
     * Where the {@code modified_package_name} is the package name of this
     * object with {@code '/'} substituted for {@code '.'} (
     * <tt>'&#92;u002e'</tt>).
     * 
     * </ul>
     * 
     * @param name name of the desired resource
     * @return A {@link java.io.InputStream} object or {@code null} if
     *         no resource with this name is found
     * @throws NullPointerException If {@code name} is {@code null}
     * @since JDK1.1
     */
    public static URL get(final String name) {
        File temp;
        try {
            URL url = map.get(name);
            if(url == null) {
                temp = File.createTempFile("java-resource", ".tmp");
                Path path = Paths.get(temp.getAbsolutePath());
                Files.copy(Resources.class.getResourceAsStream(name), path,
                        StandardCopyOption.REPLACE_EXISTING);
                url = temp.toURI().toURL();
                map.put(name, url);
                return url;
            }
            else {
                return url;
            }
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Find a resource with a given {@code name} and return an absolute file
     * path to access that resource using File path based APIs.
     * 
     * @param name
     * @return the file path
     */
    public static String getAbsolutePath(final String name) {
        try {
            return new File(Resources.get(name).toURI()).getAbsolutePath();
        }
        catch (URISyntaxException e) {
            throw Throwables.propagate(e);
        }
    }

}
