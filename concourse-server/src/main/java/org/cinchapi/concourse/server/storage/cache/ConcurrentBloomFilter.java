package org.cinchapi.concourse.server.storage.cache;

import org.cinchapi.concourse.server.concurrent.ReadWriteSharedLock;
import org.cinchapi.concourse.server.io.Byteable;
import org.cinchapi.concourse.server.io.Composite;

/**
 * A wrapper around a {@link com.google.common.hash.BloomFilter} with methods to
 * make it easier to add one or more {@link Byteable} objects
 * to the filter at a time while abstracting away the notion of
 * funnels, etc.
 * </p>
 * 
 * @author dubex
 *
 */
public class ConcurrentBloomFilter extends BloomFilter {

    /**
     * Construct a new instance.
     * 
     * @param file
     * @param source
     */
    private ConcurrentBloomFilter(String file,
            com.google.common.hash.BloomFilter<Composite> source) {
        super(file, source);
    }

    /**
     * Construct a new instance.
     * 
     * @param expectedInsertions
     */
    private ConcurrentBloomFilter(String file, int expectedInsertions) {
        super(file, expectedInsertions);
    }

    /**
     * Lock used to ensure the object is ThreadSafe. This lock provides access
     * to a lock.readLock.lock() and lock.writeLock().lock().
     */
    private ReadWriteSharedLock lock = new ReadWriteSharedLock();

    /**
     * Grabs the write {@link ReadWriteSharedLock} {@code lock} and return the
     * {@code 0L}.
     * 
     * @return {@code stamp}
     */
    protected long writeLock() {
        lock.writeLock().lock();;
        return 0L;
    }

    /**
     * Release the write {@link ReadWriteSharedLock} {@code lock}.
     * 
     * @param {@code stamp}
     */
    protected void unlockWrite(long stamp) {
        lock.writeLock().unlock();
    }

    /**
     * Always returns 0L
     * 
     */
    protected long tryOptimisticRead() {
        return 0L;
    }

    /**
     * Always returns true irrespective of {@code stamp}.
     * 
     * @param {@code stamp}
     * @return boolean
     */
    protected boolean validate(long stamp) {
        return true;
    }

    /**
     * Grabs the read {@link ReadWriteSharedLock} {@code lock}.
     * 
     * @return {@code stamp}
     */
    protected long readLock() {
        lock.readLock().lock();
        return 0L;
    }

    /**
     * Release the read {@link ReadWriteSharedLock} {@code lock}.
     * 
     * @param {@code stamp}
     */
    protected void unlockRead(long stamp) {
        lock.readLock().unlock();
    }

}