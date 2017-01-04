/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse.server.io;

import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;

/**
 * An abstraction for the reflection hack that allows us to use internal JVM
 * cleaners to free memory "directly".
 * 
 * @author Jeff Nelson
 */
public final class Cleaners {

    /**
     * Force clean the {@code buffer} and unmap it from memory, if possible.
     * This method is not guaranteed to work and can cause bad things to
     * happen, so use with caution.
     * <p>
     * Inspired by <a href="http://stackoverflow.com/a/19447758/1336833">
     * http://stackoverflow.com/a/19447758/1336833</a>}
     * </p>
     * 
     * @param buffer
     */
    public static void freeMappedByteBuffer(MappedByteBuffer buffer) {
        if(supported) {
            try {
                sunMiscCleanerCleanMethod
                        .invoke(directByteBufferGetCleanerMethod.invoke(buffer));
            }
            catch (Exception e) {}
        }
    }

    // Reflection goodness
    private static Method sunMiscCleanerCleanMethod;
    private static Method directByteBufferGetCleanerMethod;
    private static boolean supported;
    static {
        try {
            directByteBufferGetCleanerMethod = Class.forName(
                    "java.nio.DirectByteBuffer").getMethod("cleaner");
            directByteBufferGetCleanerMethod.setAccessible(true);
            sunMiscCleanerCleanMethod = Class.forName("sun.misc.Cleaner")
                    .getMethod("clean");
            sunMiscCleanerCleanMethod.setAccessible(true);
            supported = true;
        }
        catch (Exception e) {
            supported = false;
        }
    }

    private Cleaners() {/* noop */}
}
