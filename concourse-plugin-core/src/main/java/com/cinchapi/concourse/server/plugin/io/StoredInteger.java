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
package com.cinchapi.concourse.server.plugin.io;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.channels.FileLock;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.concurrent.Immutable;

import com.google.common.base.Throwables;

/**
 * A 32-bit integer value that is maintained within a {@link MappedByteBuffer
 * mappable} {@link FileChannel location}.
 * <p>
 * A {@link StoredInteger} is useful for cases when an int is constantly read
 * and updated from a disjointed file. For example, the file might contain
 * discreet messages and an int that occupies the first 4 bytes of the file is
 * maintained to specify how many messages are in the file. In such a case,
 * using a {@link StoredInteger} is useful because the value that maintains the
 * number of messages can be managed in a more natural way while ensuring atomic
 * safety.
 * </p>
 * 
 * @author Jeff Nelson
 */
@Immutable
public final class StoredInteger {

    /**
     * The number of bytes in the backing {@link #storage} needed for the
     * {@link StoredInteger}.
     */
    private final static int SIZE = 4;

    /**
     * The backing {@link FileChannel} from which the {@link #storage} buffer is
     * created; used to facilitate locking.
     */
    private final FileChannel channel;

    /**
     * The position in the backing {@link #channel} where this
     * {@link StoredInteger} begins; used to facilitate locking.
     */
    private final long position;

    /**
     * A memory-mapped segment of the backing {@link #channel} that contains the
     * represented value in stored form.
     */
    private final MappedByteBuffer storage;

    /**
     * A flag that indicates whether the integer is being used. This helps to
     * ensure the safety of atomic updates.
     */
    private AtomicBoolean using = new AtomicBoolean(false);

    /**
     * Construct a new instance.
     * 
     * @param channel the {@link FileChannel} where the value is stored
     */
    public StoredInteger(FileChannel channel) {
        this(channel, 0);
    }

    /**
     * Construct a new instance.
     * 
     * @param channel the {@link FileChannel} where the value is stored
     * @param position the position in the {@code channel} where the value
     *            begins
     */
    public StoredInteger(FileChannel channel, int position) {
        try {
            this.channel = channel;
            this.position = position;
            this.storage = channel.map(MapMode.READ_WRITE, position, 4);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Construct a new instance.
     * 
     * @param address the path to the location where the value is stored
     */
    public StoredInteger(String address) {
        this(address, 0);
    }

    /**
     * Construct a new instance.
     * 
     * @param address the path to the location where the value is stored
     * @param position the position in the {@code address} where the value
     *            begins
     */
    public StoredInteger(String address, int position) {
        File file = new File(address);
        try {
            this.channel = FileChannel.open(file.toPath(),
                    StandardOpenOption.CREATE, StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
            this.position = position;
            this.storage = channel.map(MapMode.READ_WRITE, position, 4);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Add {@code amount} to the current value and return the sum.
     * 
     * @param amount the number to add to the current value
     * @return the sum of the current value and the added {@code amount}, which
     *         becomes the new value
     */
    public int addAndGet(int amount) {
        FileLock lock = null;
        try {
            lock = channel.lock(position, SIZE, false);
            int value = getUnsafe();
            value = value + amount;
            return setUnsafe(value);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        finally {
            FileLocks.release(lock);
        }

    }

    @Override
    public void finalize() {
        try {
            channel.close();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Return the current value.
     * 
     * @return the current value
     */
    public int get() {
        while (!using.compareAndSet(false, true)) {
            continue;
        }
        FileLock lock = null;
        try {
            lock = channel.lock(position, 4, true);
            return getUnsafe();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        finally {
            FileLocks.release(lock);
            using.set(false);
        }
    }

    /**
     * Return the current value without grabbing any locks.
     * <p>
     * ONLY USE THIS METHOD INTERNALLY!!!
     * </p>
     * 
     * @return the current value
     */
    public int getUnsafe() {
        int value = storage.getInt();
        storage.rewind();
        return value;
    }

    /**
     * Set the value equal to {@code value}
     * 
     * @param value the new value
     */
    public void set(int value) {
        while (!using.compareAndSet(false, true)) {
            continue;
        }
        FileLock lock = null;
        try {
            lock = channel.lock(position, 4, false);
            setUnsafe(value);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        finally {
            FileLocks.release(lock);
            using.set(false);
        }
    }

    /**
     * Atomically set the value equal to {@code value} and {@link #sync()} the
     * change to disk.
     * 
     * @param value the new value
     */
    public void setAndSync(int value) {
        while (!using.compareAndSet(false, true)) {
            continue;
        }
        FileLock lock = null;
        try {
            lock = channel.lock(position, 4, false);
            setUnsafe(value);
            sync();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        finally {
            FileLocks.release(lock);
            using.set(false);
        }
    }

    /**
     * Force sync any changes made to disk.
     */
    public void sync() {
        storage.force();
    }

    /**
     * Set {@code value} without grabbing any locks.
     * <p>
     * ONLY USE THIS METHOD INTERNALLY!!!
     * </p>
     * 
     * @param value the value to set
     * @return {@code value} for chaining
     */
    private int setUnsafe(int value) {
        storage.putInt(value);
        storage.rewind();
        return value;
    }
}
