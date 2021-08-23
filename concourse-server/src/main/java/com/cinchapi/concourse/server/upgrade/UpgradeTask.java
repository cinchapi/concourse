/*
 * Copyright (c) 2013-2021 Cinchapi Inc.
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
package com.cinchapi.concourse.server.upgrade;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.channels.FileLock;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.concourse.annotate.Restricted;
import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.util.Logger;
import com.cinchapi.concourse.util.Numbers;
import com.cinchapi.concourse.util.Versions;

/**
 * An {@link UpgradeTask} performs an operation that "upgrades" previously
 * stored data. All tasks that are defined in the
 * {@code com.cinchapi.concourse.server.upgrade.task} package and are
 * higher than the current system version are automatically run by the
 * {@link Upgrader} whenever the user upgrades the ConcourseServer.
 * 
 * @author Jeff Nelson
 */
public abstract class UpgradeTask implements Comparable<UpgradeTask> {

    /**
     * Return the current system version.
     * 
     * @return the current storage version
     */
    public static int getCurrentSystemVersion() {
        return Numbers.min(getHomeSystemVersion(), getBufferSystemVersion(),
                getDbSystemVersion()).intValue();
    }

    /**
     * Update the system version of the Concourse Server installation to
     * {@code version}.
     * 
     * @param version
     */
    @Restricted
    public static void setCurrentSystemVersion(int version) {
        setBufferCurrentSystemVersion(version);
        setDatabaseCurrentSystemVersion(version);
        setHomeCurrentSystemVersion(version);
    }

    /**
     * Update the system version in the buffer data files.
     * <p>
     * <strong>WARNING:</strong> Setting the system version in individual
     * locations is not recommended because lack of consensus about the system
     * version can cause unexpected results. It is best to use the
     * {@link #setCurrentSystemVersion(int)} method unless this method is being
     * called for a known special case.
     * <p>
     * 
     * @param version
     */
    @Restricted
    static void compareAndSetBufferCurrentSystemVersion(int expectedVersion,
            int version) {
        compareAndWriteCurrentSystemVersion(BUFFER_VERSION_FILE,
                expectedVersion, version);
    }

    /**
     * Update the system version in the database data files.
     * <p>
     * <strong>WARNING:</strong> Setting the system version in individual
     * locations is not recommended because lack of consensus about the system
     * version can cause unexpected results. It is best to use the
     * {@link #setCurrentSystemVersion(int)} method unless this method is being
     * called for a known special case.
     * <p>
     * 
     * @param version
     */
    @Restricted
    static void compareAndSetDatabaseCurrentSystemVersion(int expectedVersion,
            int version) {
        compareAndWriteCurrentSystemVersion(DB_VERSION_FILE, expectedVersion,
                version);
    }

    /**
     * Update the system version in the home directory.
     * <p>
     * <strong>WARNING:</strong> Setting the system version in individual
     * locations is not recommended because lack of consensus about the system
     * version can cause unexpected results. It is best to use the
     * {@link #setCurrentSystemVersion(int)} method unless this method is being
     * called for a known special case.
     * <p>
     * 
     * @param version
     */
    @Restricted
    static void compareAndSetHomeCurrentSystemVersion(int expectedVersion,
            int version) {
        compareAndWriteCurrentSystemVersion(HOME_VERSION_FILE, expectedVersion,
                version);
    }

    /**
     * Update the system version in the buffer data files.
     * <p>
     * <strong>WARNING:</strong> Setting the system version in individual
     * locations is not recommended because lack of consensus about the system
     * version can cause unexpected results. It is best to use the
     * {@link #setCurrentSystemVersion(int)} method unless this method is being
     * called for a known special case.
     * <p>
     * 
     * @param version
     */
    @Restricted
    static void setBufferCurrentSystemVersion(int version) {
        writeCurrentSystemVersion(BUFFER_VERSION_FILE, version);
    }

    /**
     * Update the system version in the database data files.
     * <p>
     * <strong>WARNING:</strong> Setting the system version in individual
     * locations is not recommended because lack of consensus about the system
     * version can cause unexpected results. It is best to use the
     * {@link #setCurrentSystemVersion(int)} method unless this method is being
     * called for a known special case.
     * <p>
     * 
     * @param version
     */
    @Restricted
    static void setDatabaseCurrentSystemVersion(int version) {
        writeCurrentSystemVersion(DB_VERSION_FILE, version);
    }

    /**
     * Update the system version in the server home directory.
     * <p>
     * <strong>WARNING:</strong> Setting the system version in individual
     * locations is not recommended because lack of consensus about the system
     * version can cause unexpected results. It is best to use the
     * {@link #setCurrentSystemVersion(int)} method unless this method is being
     * called for a known special case.
     * <p>
     * 
     * @param version
     */
    @Restricted
    static void setHomeCurrentSystemVersion(int version) {
        writeCurrentSystemVersion(HOME_VERSION_FILE, version);
    }

    /**
     * Atomically write out the system {@code version} to {@code file} if the
     * current version in {@code file} is equal to {@code expectedVersion}.
     * 
     * @param file
     * @param expectedVersion
     * @param version
     */
    private static void compareAndWriteCurrentSystemVersion(String file,
            int expectedVersion, int version) {
        FileChannel channel = FileSystem.getFileChannel(file);
        try {
            MappedByteBuffer bytes = channel.map(MapMode.READ_WRITE, 0, 4)
                    .load();
            FileLock lock = channel.lock();
            try {
                int currentVersion = bytes.getInt();
                if(currentVersion == expectedVersion) {
                    bytes.flip();
                    bytes.putInt(version);
                    bytes.force();
                }
            }
            finally {
                lock.release();
            }
        }
        catch (IOException e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
        finally {
            FileSystem.closeFileChannel(channel);
        }
    }

    /**
     * Return the current buffer system version.
     * 
     * @return the buffer system
     */
    private static int getBufferSystemVersion() {
        if(FileSystem.hasFile(BUFFER_VERSION_FILE)) {
            return FileSystem.map(BUFFER_VERSION_FILE, MapMode.READ_ONLY, 0, 4)
                    .getInt();
        }
        else {
            return 0;
        }
    }

    /**
     * Return the current database system version.
     * 
     * @return the db system version
     */
    private static int getDbSystemVersion() {
        if(FileSystem.hasFile(DB_VERSION_FILE)) {
            return FileSystem.map(DB_VERSION_FILE, MapMode.READ_ONLY, 0, 4)
                    .getInt();
        }
        else {
            return 0;
        }
    }

    /**
     * Return the current home system version.
     * 
     * @return the home system version
     */
    private static int getHomeSystemVersion() {
        if(FileSystem.hasFile(HOME_VERSION_FILE)) {
            return FileSystem.map(VERSION_FILE_NAME, MapMode.READ_ONLY, 0, 4)
                    .getInt();
        }
        else {
            return 0;
        }
    }

    /**
     * Write out the system {@code version} to the {@code file}.
     * 
     * @param file
     * @param version
     */
    private static void writeCurrentSystemVersion(String file, int version) {
        ((MappedByteBuffer) FileSystem.map(file, MapMode.READ_WRITE, 0, 4)
                .putInt(version)).force();
    }

    /**
     * The name of the file we use to hold the internal system version of the
     * most recently run upgrade task.
     */
    private static String VERSION_FILE_NAME = ".schema";

    /**
     * The name of the file we use to hold the the internal system version of
     * the most recently run upgrade task in the Buffer.
     */
    private static final String BUFFER_VERSION_FILE = GlobalState.BUFFER_DIRECTORY
            + File.separator + VERSION_FILE_NAME;

    /**
     * The name of the file we use to hold the internal system version of the
     * most recently run upgrade task in the Database.
     */
    private static final String DB_VERSION_FILE = GlobalState.DATABASE_DIRECTORY
            + File.separator + VERSION_FILE_NAME;

    /**
     * The name of the file that we used to hold the internal system version of
     * the most recently run upgrade task in the home directory.
     */
    private static final String HOME_VERSION_FILE = GlobalState.CONCOURSE_HOME
            + File.separator + VERSION_FILE_NAME;

    @Override
    public int compareTo(UpgradeTask o) {
        return Integer.compare(version(), o.version());
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof UpgradeTask) {
            return version() == ((UpgradeTask) obj).version();
        }
        else {
            return false;
        }
    }

    /**
     * Return a description of the upgrade task that informs the user about the
     * operation.
     * 
     * @return the description
     */
    public abstract String getDescription();

    @Override
    public int hashCode() {
        return version();
    }

    /**
     * Run the upgrade task.
     */
    public void run() {
        logInfoMessage("STARTING {}", this);
        try {
            doTask();
            setCurrentSystemVersion(version());
            logInfoMessage("FINISHED {}", this);
        }
        catch (Exception e) {
            logErrorMessage("FAILED {}: {}", this, e);
            throw e;
        }
    }

    @Override
    public String toString() {
        return "Upgrade Task " + version() + ": " + getDescription();
    }

    /**
     * Return the numerical system version of this task. Each upgrade task
     * should have a unique system version that is higher than any other task on
     * which it depends.
     * 
     * @return the storage version of this task
     */
    public abstract int version();

    /**
     * Implement the logic for the upgrade task. Any thrown exceptions will
     * cause the task to fail.
     */
    protected abstract void doTask();

    /**
     * Rollback the work done by this {@link UpgradeTask} so that the system is
     * in a state that is consistent with its state before the task ws
     * attempted.
     */
    protected abstract void rollback();

    /**
     * Return the path to the server installation directory, from which other
     * aspects of the Concourse Server deployment are accessible. This is
     * typically the working directory from which Concourse Server is launched.
     */
    protected String getServerInstallDirectory() {
        return System.getProperty("user.dir");
    }

    /**
     * Print a DEBUG log message.
     * 
     * @param message
     * @param params
     */
    protected final void logDebugMessage(String message, Object... params) {
        Logger.upgradeDebug(decorateLogMessage(message), params);
    }

    /**
     * Print an ERROR log message.
     * 
     * @param message
     * @param params
     */
    protected final void logErrorMessage(String message, Object... params) {
        Logger.upgradeError(decorateLogMessage(message), params);
    }

    /**
     * Print an INFO log message.
     * 
     * @param message
     * @param params
     */
    protected final void logInfoMessage(String message, Object... params) {
        Logger.upgradeInfo(decorateLogMessage(message), params);
    }

    /**
     * Print a WARN log message.
     * 
     * @param message
     * @param params
     */
    protected final void logWarnMessage(String message, Object... params) {
        Logger.upgradeWarn(decorateLogMessage(message), params);
    }

    /**
     * Return the minimum schema version that is required for this
     * {@link UpgradeTask} to run.
     * 
     * @return the minimum required version
     */
    protected int requiresVersion() {
        return (int) Versions.toLongRepresentation("0.10.2.1", 2);
    }

    /**
     * Decorate the log {@code message} to conform the upgrade task
     * identification standards.
     * 
     * @param message
     * @return the formatted log message
     */
    private String decorateLogMessage(String message) {
        return "Upgrade(" + version() + "): " + message;
    }

}
