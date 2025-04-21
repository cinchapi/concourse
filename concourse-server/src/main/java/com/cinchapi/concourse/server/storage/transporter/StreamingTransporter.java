/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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
package com.cinchapi.concourse.server.storage.transporter;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.concourse.server.storage.db.Database;
import com.cinchapi.concourse.server.storage.temp.Buffer;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Logger;

/**
 * A {@link StreamingTransporter} incrementally transports writes from a
 * {@link Buffer} to a {@link Database} concurrently within other read/write
 * operations so that the cost and overhead are amortized to create predictable
 * and consistent overall system throughput.
 * <p>
 * The {@link StreamingTransporter} balances the competing resource demands of
 * transport and normal operations. Since reads cannot complete when data is
 * being transported from the buffer to the database, the transporter employs
 * adaptive strategies to minimize interference:
 * <ul>
 * <li>During high write activity, it streams data continuously but in
 * controlled batches</li>
 * <li>During periods of read activity, it scales back transport operations</li>
 * <li>During low activity, it pauses to conserve resources</li>
 * <li>It includes self-monitoring to detect and recover from stalls</li>
 * </ul>
 * </p>
 * <p>
 * This implementation is a critical component of Concourse's storage
 * architecture, ensuring that data written to the Buffer is eventually
 * accessible in the Database for efficient querying while maintaining system
 * responsiveness. By spreading transport work across normal operations, it
 * prevents the system from experiencing large transport-related pauses.
 * </p>
 *
 * @author Jeff Nelson
 */
public class StreamingTransporter extends Transporter {

    /**
     * Creates a thread name for the transport thread based on the environment.
     *
     * @param environment the environment name
     * @return the formatted thread name
     */
    private static String threadNameFormat(String environment) {
        return AnyStrings.joinSimple("BufferTransport [", environment, "]");
    }

    /**
     * Creates an exception handler for the transport thread.
     *
     * @return the uncaught exception handler
     */
    private static UncaughtExceptionHandler uncaughtExceptionHandler() {
        return (thread, exception) -> {
            Logger.error("Uncaught exception in {}:", thread.getName(),
                    exception);
            Logger.error(
                    "{} has STOPPED WORKING due to an unexpected exception."
                            + "Writes will accumulate in the buffer without being "
                            + "transported until the error is resolved",
                    thread.getName());
        };
    }

    /**
     * The number of milliseconds allowed between writes before pausing the
     * transport thread. If the time between writes is less than this value,
     * the thread will busy-wait rather than block, which is more efficient
     * for streaming writes.
     */
    protected static final int BUFFER_TRANSPORT_THREAD_ALLOWABLE_INACTIVITY_THRESHOLD_IN_MILLISECONDS = 1000; // visible
                                                                                                              // for
                                                                                                              // testing

    /**
     * The frequency with which the system checks if the transport thread
     * has hung or stalled.
     */
    protected static int BUFFER_TRANSPORT_THREAD_HUNG_DETECTION_FREQUENCY_IN_MILLISECONDS = 10000; // visible
                                                                                                   // for
                                                                                                   // testing

    /**
     * The number of milliseconds allowed for the transport thread to sleep
     * without waking up before it's considered stalled and needs rescue.
     */
    protected static int BUFFER_TRANSPORT_THREAD_HUNG_DETECTION_THRESOLD_IN_MILLISECONDS = 5000; // visible
                                                                                                 // for
                                                                                                 // testing

    /**
     * The maximum number of milliseconds to sleep between transport cycles.
     */
    private static final int MAX_TRANSPORT_THREAD_SLEEP_TIME_IN_MS = 100;

    /**
     * The minimum number of milliseconds to sleep between transport cycles.
     */
    private static final int MIN_TRANSPORT_THREAD_SLEEP_TIME_IN_MS = 5;

    /**
     * The number of milliseconds to sleep between transport cycles.
     */
    private int transportThreadSleepTimeInMs = MAX_TRANSPORT_THREAD_SLEEP_TIME_IN_MS;

    /**
     * The source buffer from which writes are transported.
     */
    private final Buffer buffer;

    /**
     * The destination database where transported writes are stored.
     */
    private final Database database;

    /**
     * A flag that indicates whether the transport thread is actively doing
     * work. This flag prevents interrupting the thread if it appears to be
     * hung when it is actually just busy processing a large amount of data.
     */
    private final AtomicBoolean bufferTransportThreadIsDoingWork = new AtomicBoolean(
            false);

    /**
     * A flag that indicates that the transport thread is currently paused
     * due to inactivity (e.g., no writes to process).
     */
    private final AtomicBoolean bufferTransportThreadIsPaused = new AtomicBoolean(
            false);

    /**
     * The timestamp when the transport thread last awoke from sleep.
     * Used to monitor and detect whether the thread has stalled or hung.
     */
    private final AtomicLong bufferTransportThreadLastWakeUp = new AtomicLong(
            Time.now());

    /**
     * A flag indicating whether the transport thread has ever been
     * successfully restarted after appearing to be hung.
     */
    protected final AtomicBoolean bufferTransportThreadHasEverBeenRestarted = new AtomicBoolean(
            false); // visible for testing

    /**
     * A flag indicating whether the transport thread has ever entered
     * "paused" mode where it blocks during inactivity instead of busy waiting.
     */
    protected final AtomicBoolean bufferTransportThreadHasEverPaused = new AtomicBoolean(
            false); // visible for testing

    /**
     * If this value is > 0, the thread will sleep for this amount instead
     * of what the buffer suggests. This is mainly used for testing.
     */
    protected int bufferTransportThreadSleepInMs = 0; // visible for testing

    /**
     * The lock used to coordinate access during critical sections.
     */
    private final Lock lock;

    /**
     * A {@link Timer} that is used to schedule some regular tasks.
     */
    private Timer hungTaskDetector;

    /**
     * A flag to indicate that the {@link BufferTransportThrread} has appeared
     * to be hung at some point during the current runtime.
     */
    protected final AtomicBoolean bufferTransportThreadHasEverAppearedHung = new AtomicBoolean(
            false); // visible for testing

    /**
     * Constructs a new {@link StreamingTransporter} that streams writes from
     * the specified buffer to the database.
     *
     * @param buffer the source buffer containing writes to transport
     * @param database the destination database for storage
     * @param environment the environment name for thread identification
     * @param lock the lock used to coordinate access during critical sections
     */
    public StreamingTransporter(Buffer buffer, Database database,
            String environment, Lock lock) {
        super(threadNameFormat(environment), uncaughtExceptionHandler());
        this.buffer = buffer;
        this.database = database;
        this.lock = lock;
        buffer.onTransportRateScaleBack(
                () -> transportThreadSleepTimeInMs = MAX_TRANSPORT_THREAD_SLEEP_TIME_IN_MS);
    }

    @Override
    public void start() {
        hungTaskDetector = new Timer(true);
        hungTaskDetector.scheduleAtFixedRate(new TimerTask() {

            @Override
            public void run() {
                if(transportTaskAppearsHung()) {
                    bufferTransportThreadHasEverAppearedHung.set(true);
                    restart();
                }

            }

        }, BUFFER_TRANSPORT_THREAD_HUNG_DETECTION_FREQUENCY_IN_MILLISECONDS,
                BUFFER_TRANSPORT_THREAD_HUNG_DETECTION_FREQUENCY_IN_MILLISECONDS);
        super.start();

    }

    @Override
    public void stop() {
        super.stop();
        hungTaskDetector.cancel();
        hungTaskDetector = null;
    }

    /**
     * Processes a single transport cycle, moving data from the buffer to
     * the database. This method implements adaptive behavior to balance
     * transport with normal operations:
     * <ul>
     * <li>Pauses during periods of inactivity to conserve resources</li>
     * <li>Performs transports in controlled batches to allow reads to
     * proceed</li>
     * <li>Adjusts sleep times based on system activity</li>
     * <li>Includes self-healing for stalled or hung states</li>
     * </ul>
     *
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    @Override
    public void transport() throws InterruptedException {
        if(getBufferTransportThreadIdleTimeInMs() > BUFFER_TRANSPORT_THREAD_ALLOWABLE_INACTIVITY_THRESHOLD_IN_MILLISECONDS) {
            // If there have been no transports within the last second
            // then make this thread block until the buffer is
            // transportable so that we do not waste CPU cycles
            // busy waiting unnecessarily.
            bufferTransportThreadHasEverPaused.set(true);
            bufferTransportThreadIsPaused.set(true);
            Logger.debug("Paused the background data transport thread because "
                    + "it has been inactive for at least {} milliseconds",
                    BUFFER_TRANSPORT_THREAD_ALLOWABLE_INACTIVITY_THRESHOLD_IN_MILLISECONDS);
            buffer.waitUntilTransportable();
            if(Thread.interrupted()) { // the thread has been
                                       // interrupted from the Engine
                                       // stopping
                return;
            }
        }
        doTransport();
        try {
            // NOTE: This thread needs to sleep for a small amount of
            // time to avoid thrashing
            int sleep = bufferTransportThreadSleepInMs > 0
                    ? bufferTransportThreadSleepInMs
                    : transportThreadSleepTimeInMs;
            Thread.sleep(sleep);
            bufferTransportThreadLastWakeUp.set(Time.now());
        }
        catch (InterruptedException e) {
            if(getBufferTransportThreadIdleTimeInMs() > BUFFER_TRANSPORT_THREAD_HUNG_DETECTION_THRESOLD_IN_MILLISECONDS) {
                Logger.warn("The data transport thread been sleeping for over "
                        + "{} milliseconds even though there is work to do. "
                        + "An attempt has been made to restart the stalled "
                        + "process.",
                        BUFFER_TRANSPORT_THREAD_HUNG_DETECTION_THRESOLD_IN_MILLISECONDS);
                bufferTransportThreadHasEverBeenRestarted.set(true);
            }
            else {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Performs the actual transport operation from buffer to database,
     * using a non-blocking approach to prevent deadlocks and minimize
     * interference with normal read/write operations.
     */
    private void doTransport() {
        if(lock.tryLock()) {
            try {
                bufferTransportThreadIsPaused.compareAndSet(true, false);
                bufferTransportThreadIsDoingWork.set(true);
                if(buffer.tryTransport(database)) {
                    --transportThreadSleepTimeInMs;
                    if(transportThreadSleepTimeInMs < MIN_TRANSPORT_THREAD_SLEEP_TIME_IN_MS) {
                        transportThreadSleepTimeInMs = MIN_TRANSPORT_THREAD_SLEEP_TIME_IN_MS;
                    }
                }
                bufferTransportThreadLastWakeUp.set(Time.now());
                bufferTransportThreadIsDoingWork.set(false);
            }
            finally {
                lock.unlock();
            }
        }
    }

    /**
     * Returns the number of milliseconds that have elapsed since the last
     * successful transport operation.
     *
     * @return the idle time in milliseconds
     */
    private long getBufferTransportThreadIdleTimeInMs() {
        return TimeUnit.MILLISECONDS.convert(
                Time.now() - buffer.getTimeOfLastTransport(),
                TimeUnit.MICROSECONDS);
    }

    /**
     * Return {@code true} if it appears that the {@link #transport()} task
     * appears hung.
     * 
     * @return {@code true} if the task appears hung
     */
    private final boolean transportTaskAppearsHung() {
        if(!bufferTransportThreadIsDoingWork.get()) {
            if(!bufferTransportThreadIsPaused.get()) {
                if(bufferTransportThreadLastWakeUp.get() != 0) {
                    long elapsedMs = TimeUnit.MILLISECONDS.convert(
                            Time.now() - bufferTransportThreadLastWakeUp.get(),
                            TimeUnit.MICROSECONDS);
                    return elapsedMs > BUFFER_TRANSPORT_THREAD_HUNG_DETECTION_THRESOLD_IN_MILLISECONDS;
                }
            }
        }
        return false;
    }
}
