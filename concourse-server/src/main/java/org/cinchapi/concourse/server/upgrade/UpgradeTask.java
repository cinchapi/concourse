/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2014 Jeff Nelson, Cinchapi Software Collective
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
package org.cinchapi.concourse.server.upgrade;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;

import org.cinchapi.concourse.server.GlobalState;
import org.cinchapi.concourse.server.io.FileSystem;
import org.cinchapi.concourse.util.Logger;

/**
 * An {@link UpgradeTask} performs an operation that "upgrades" previously
 * stored data. All tasks that are defined in the
 * {@code org.cinchapi.concourse.server.upgrade.task} package and are
 * higher than the current system version are automatically run by the
 * {@link Upgrader} whenever the user upgrades the ConcourseServer.
 * 
 * @author jnelson
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
        return Integer.compare(getSystemVersion(), o.getSystemVersion());
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof UpgradeTask) {
            return getSystemVersion() == ((UpgradeTask) obj).getSystemVersion();
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

    /**
     * Return the numerical system version of this task. Each upgrade task
     * should have a unique system version that is higher than any other task on
     * which it depends.
     * 
     * @return the storage version of this task
     */
    public abstract int getSystemVersion();

    @Override
    public int hashCode() {
        return getSystemVersion();
    }

    /**
     * Run the upgrade task.
     */
    public void run() {
        logInfoMessage("STARTING {}", this);
        try {
            doTask();
            propagateSystemVersionUpdate();
            logInfoMessage("FINISHED {}", this);
        }
        catch (Exception e) {
            logErrorMessage("FAILED {}: {}", this, e);
            throw e;
        }
    }

    @Override
    public String toString() {
        return "Upgrade Task " + getSystemVersion() + ": " + getDescription();
    }

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
     * Update the system version of the Concourse Server installation to that of
     * this upgrade task.
     */
    void propagateSystemVersionUpdate() {
        ((MappedByteBuffer) FileSystem.map(BUFFER_VERSION_FILE,
                MapMode.READ_WRITE, 0, 4).putInt(getSystemVersion())).force();
        ((MappedByteBuffer) FileSystem.map(DB_VERSION_FILE, MapMode.READ_WRITE,
                0, 4).putInt(getSystemVersion())).force();
    }

    /**
     * Decorate the log {@code message} to conform the upgrade task
     * identification standards.
     * 
     * @param message
     * @return the formatted log message
     */
    private String decorateLogMessage(String message) {
        return "Upgrade(" + getSystemVersion() + "): " + message;
    }

}
