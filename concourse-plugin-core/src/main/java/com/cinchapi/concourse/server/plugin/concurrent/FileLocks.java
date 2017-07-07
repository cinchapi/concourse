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
package com.cinchapi.concourse.server.plugin.concurrent;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

import javax.annotation.Nullable;

import com.google.common.base.Throwables;

/**
 * A util class for dealing with {@link FileLock} objects.
 * 
 * @author Jeff Nelson
 */
public class FileLocks {

    /**
     * Return the {@link FileLock} for {@code channel} starting at
     * {@code position} and extending for {@code length} bytes.
     * 
     * @param channel
     * @param position
     * @param length
     * @param shared
     * @return the {@link FileLock}
     */
    public static FileLock lock(FileChannel channel, long position, long length,
            boolean shared) {
        return new SpinningFileLock(channel, position, length, shared);
    }

    /**
     * Release the {@code lock} quietly, if it is not equal to {@code null}.
     * 
     * @param lock a hopefully non-null {@link FileLock} that needs to be
     *            released
     */
    public static void release(@Nullable FileLock lock) {
        if(lock != null) {
            try {
                lock.release();
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

}
