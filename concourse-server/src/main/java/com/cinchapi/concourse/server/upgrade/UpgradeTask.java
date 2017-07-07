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
package com.cinchapi.concourse.server.upgrade;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;

import com.cinchapi.concourse.annotate.Restricted;
import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.util.Logger;

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
        return Math.min(getBufferSystemVersion(), getDbSystemVersion());
    }

    /**
     * Update the system version of the Concourse Server installation to
     * {@code version}.
     * 
     * @param version
     */
    @Restricted
    public static void setCurrentSystemVersion(int version) {
        ((MappedByteBuffer) FileSystem.map(BUFFER_VERSION_FILE,
                MapMode.READ_WRITE, 0, 4).putInt(version)).force();
        ((MappedByteBuffer) FileSystem.map(DB_VERSION_FILE, MapMode.READ_WRITE,
                0, 4).putInt(version)).force();
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
     * @return the db system
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
     * The name of the file we use to hold the internal system version of the
     * most recently run upgrade task.
     */
    private static String VERSION_FILE_NAME = ".schema";

    /**
     * The name of the file we use to hold the internal system version of the
     * most recently run upgrade task in the Database.
     */
    private static final String DB_VERSION_FILE = GlobalState.DATABASE_DIRECTORY
            + File.separator + VERSION_FILE_NAME;

    /**
     * The name of the file we use to hold the the internal system version of
     * the most recently run upgrade task in the Buffer.
     */
    private static final String BUFFER_VERSION_FILE = GlobalState.BUFFER_DIRECTORY
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
        Logger.debug(decorateLogMessage(message), params);
    }

    /**
     * Print an ERROR log message.
     * 
     * @param message
     * @param params
     */
    protected final void logErrorMessage(String message, Object... params) {
        Logger.error(decorateLogMessage(message), params);
    }

    /**
     * Print an INFO log message.
     * 
     * @param message
     * @param params
     */
    protected final void logInfoMessage(String message, Object... params) {
        Logger.info(decorateLogMessage(message), params);
    }

    /**
     * Print a WARN log message.
     * 
     * @param message
     * @param params
     */
    protected final void logWarnMessage(String message, Object... params) {
        Logger.warn(decorateLogMessage(message), params);
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
