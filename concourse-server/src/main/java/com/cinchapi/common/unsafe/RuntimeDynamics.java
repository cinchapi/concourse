/*
 * Copyright (c) 2016 Cinchapi Inc.
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
package com.cinchapi.common.unsafe;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.Collections;

import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.ToolProvider;

/**
 * Some runtime hacks with very narrow use cases.
 * 
 * @author Jeff Nelson
 */
public final class RuntimeDynamics { // Copied from accent4j

    /**
     * Make a best effort to return an object that belongs to a class that is
     * dynamically created at runtime.
     * 
     * @return the anonymous object
     */
    @SuppressWarnings({ "unchecked", "restriction", "rawtypes" })
    public static Object newAnonymousObject() {
        try {
            StringBuilder source = new StringBuilder();
            String clazz = "A" + System.currentTimeMillis();
            source.append("public class ").append(clazz).append(" {}");
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            SimpleJavaFileObject file = new SimpleJavaFileObject(
                    URI.create(clazz + ".java"),
                    javax.tools.JavaFileObject.Kind.SOURCE) {

                @Override
                public CharSequence getCharContent(
                        boolean ignoreEncodingErrors) {
                    return source.toString();
                }

                @Override
                public OutputStream openOutputStream() throws IOException {
                    return output;
                }
            };
            JavaFileManager manager = new ForwardingJavaFileManager(
                    ToolProvider.getSystemJavaCompiler()
                            .getStandardFileManager(null, null, null)) {

                @Override
                public JavaFileObject getJavaFileForOutput(Location location,
                        String className, JavaFileObject.Kind kind,
                        FileObject sibling) throws IOException {
                    return file;
                }
            };
            ToolProvider.getSystemJavaCompiler().getTask(null, manager, null,
                    null, null, Collections.singletonList(file)).call();
            final byte[] bytes = output.toByteArray();

            final Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            final sun.misc.Unsafe unsafe = (sun.misc.Unsafe) f.get(null);
            final Class<?> anonymous = unsafe.defineClass(clazz, bytes, 0,
                    bytes.length, null, null);
            return anonymous.newInstance();
        }
        catch (Exception e) {
            return new Object() {};
        }
    }

    private RuntimeDynamics() {/* no-op */}

}
