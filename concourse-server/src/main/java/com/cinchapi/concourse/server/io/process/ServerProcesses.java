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

import java.io.Serializable;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.commons.lang.StringUtils;

import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.util.Processes;
import com.cinchapi.concourse.util.Resources;
import com.cinchapi.concourse.util.Serializables;

/**
 * An extension of the {@link com.cinchapi.concourse.util.Processes Processes}
 * utils class with server specific capabilities.
 * 
 * @author Jeff Nelson
 */
public final class ServerProcesses {

    /**
     * The template to use for {@link #fork(Forkable) forked} routines run in a
     * separate {@link JavaApp}.
     */
    private static String FORK_TEMPLATE = FileSystem.read(Resources
            .getAbsolutePath("/META-INF/ForkRunner.tpl"));

    /**
     * Fork the {@code routine} to a separate JVM process and block until the
     * result can be returned locally.
     * 
     * <p>
     * This method makes it as easy to fork logic to a separate JVM process as
     * it is to fork to a separate thread using a {@link Runnable} or
     * {@link Callable}.
     * </p>
     * 
     * @param routine the {@link Forkable} to run in a separate process
     * @param callback the {@link Callback} that handles the result
     * @return the result of running the routine
     */
    public static <T extends Serializable> T fork(Forkable<T> routine) {
        Callback<T> callback = new NoOpCallback<T>();
        fork(routine, callback);
        return callback.getResult();
    }

    /**
     * Fork the {@code routine} to a separate JVM process and return the result
     * locally by passing it to the {@code callback}.
     * 
     * <p>
     * This method makes it as easy to fork logic to a separate JVM process as
     * it is to fork to a separate thread using a {@link Runnable} or
     * {@link Callable}.
     * </p>
     * 
     * @param routine the {@link Forkable} routine to run in a separate process
     * @param callback the {@link Callback} that handles the result
     * @return the result of running the routine
     */
    public static <T extends Serializable> void fork(final Forkable<T> routine,
            final Callback<T> callback) {
        String input = FileSystem.tempFile(); // use to serialize the #routine
                                              // so it can be read by forked
                                              // process
        final String output = FileSystem.tempFile(); // used to serialize the
                                                     // return value for the
                                                     // #routine so it be read
                                                     // by this process
        String source = FORK_TEMPLATE.replace("INSERT_INPUT_PATH", input)
                .replace("INSERT_OUTPUT_PATH", output)
                .replace("INSERT_CLASS_NAME", routine.getClass().getName());

        // Since the #routine is forked, we offer the external JVM process the
        // local classpath
        String classpath = StringUtils.join(((URLClassLoader) Thread
                .currentThread().getContextClassLoader()).getURLs(),
                JavaApp.CLASSPATH_SEPARATOR);
        FileChannel inputChannel = FileSystem.getFileChannel(input);
        try {
            Serializables.write(routine, inputChannel);
            final JavaApp app = new JavaApp(classpath, source);
            app.run();
            new Thread(new Runnable() { // Wait for completion in separate
                                        // thread so as to not block the caller

                        @Override
                        public void run() {
                            Processes.waitForSuccessfulCompletion(app);
                            ByteBuffer result = FileSystem.readBytes(output);
                            T ret = Serializables.read(result,
                                    routine.getReturnType());
                            callback.result(ret);
                        }

                    }).start();
        }
        finally {
            FileSystem.closeFileChannel(inputChannel);
        }
    }
}
