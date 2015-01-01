/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2015 Jeff Nelson, Cinchapi Software Collective
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.cinchapi.concourse.server.io;

import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;

/**
 * An abstraction for the reflection hack that allows us to use internal JVM
 * cleaners to free memory "directly".
 * 
 * @author jnelson
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