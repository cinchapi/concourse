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
import java.nio.channels.OverlappingFileLockException;
import java.util.concurrent.ConcurrentHashMap;

import com.cinchapi.common.base.CheckedExceptions;

/**
 * A {@link FileLock} that behaves in a standard fashion for cross-process lock
 * acquisitions, but acts a spin lock to gate access for threads within a single
 * JVM.
 * 
 * @author Jeff Nelson
 */
public class SpinningFileLock extends FileLock {

    /**
     * A collection of all the locks that are held by this JVM.
     * <p>
     * We track this collection separately because the JVM tends to occasionally
     * allow overlapping file locks when it shouldn't/
     * </p>
     */
    private static final ConcurrentHashMap<FileLock, Boolean> LOCK_TABLE = new ConcurrentHashMap<>();

    /**
     * The underlying {@link FileLock}.
     */
    private FileLock lock;

    /**
     * Construct a new instance.
     * 
     * @param channel
     * @param position
     * @param size
     * @param shared
     */
    protected SpinningFileLock(FileChannel channel, long position, long size,
            boolean shared) {
        super(channel, position, size, shared);
        lock = null;
        while (lock == null) {
            try {
                lock = channel.lock(position, size, shared);
                synchronized (lock) {
                    if(LOCK_TABLE.putIfAbsent(lock, true) != null) {
                        // Lock is already in the table, which means it is held
                        // by this JVM, so we should go back to blocking mode...
                        lock = null;
                    }
                }
            }
            catch (OverlappingFileLockException e) {
                // Spin on overalapping attempts to grab this lock in the same
                // JVM to simulate traditional intra-process blocking.
                Thread.yield();
                continue;
            }
            catch (IOException e) {
                throw CheckedExceptions.throwAsRuntimeException(e);
            }
        }
    }

    @Override
    public boolean isValid() {
        return lock.isValid();
    }

    @Override
    public void release() throws IOException {
        synchronized (lock) {
            LOCK_TABLE.remove(lock);
            FileLocks.release(lock);
        }
    }

}
