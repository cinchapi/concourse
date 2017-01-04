/*
 * Copyright (c) 2014, 2016 J. Lewis Muir <jlmuir@imca-cat.org>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS
 * BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

package org.imca_cat.pollingwatchservice;

import java.io.IOException;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * A {@link PathWatchService} that polls for changes.
 * <p>
 * Example:
 *
 * <pre>
 *     PathWatchService s = new PollingWatchService(4, 15, TimeUnit.SECONDS);
 *     s.register(FileSystems.getDefault().getPath("/home/luke"),
 *         StandardWatchEventKinds.ENTRY_CREATE,
 *         StandardWatchEventKinds.ENTRY_DELETE,
 *         StandardWatchEventKinds.ENTRY_MODIFY);
 *     s.start();
 *     for (;;) {
 *       WatchKey k = s.take();
 *       ...
 *     }
 * </pre>
 * <p>
 * Note: polling is only safe when operating on secure directories (i.e. ones
 * in which there is no risk of a file system attack by a malicious user). If
 * operating on an insecure directory, an attacker may be able to trick this
 * service into reporting file system changes of their choosing (e.g. an
 * ENTRY_CREATE event for a file with a name and path of the attacker's
 * choosing).
 * <p>
 * According to the CERT Secure Coding website, rule <a
 * href="https://www.securecoding.cert.org/confluence/x/ioHWAw">FIO00-J. Do not
 * operate on files in shared directories</a> (accessed on July 1, 2014):
 * <q>A directory is secure with respect to a particular user if only the user
 * and the system administrator are allowed to create, move, or delete files
 * inside the directory. Furthermore, each parent directory must itself be a
 * secure directory up to and including the root directory. On most systems,
 * home or user directories are secure by default and only shared directories
 * are insecure.</q>
 */
public class PollingWatchService implements PathWatchService {
    private final long period;
    private final TimeUnit periodUnit;
    private boolean useFileMetadata;
    private boolean started;
    private final BlockingQueue<PollingWatchKey> queue;
    private final ConcurrentHashMap<Path, PollingWatchKey> keys;
    private final ExecutorService executor;
    private final ScheduledExecutorService timer;
    private final PollingWatchServiceExceptionHandler exceptionHandler;
    private final PollingWatchServiceInfoHandler infoHandler;
    private final Object registerLock;
    private final PollingWatchKey closedSentinel;
    private final Object closeLock;
    private volatile boolean closed;
    private volatile boolean registerMethodEverInvoked;

    /**
     * Constructs a new service with the specified thread pool size, period, and
     * time unit, and with a default exception handler that invokes the
     * exception's {@code printStackTrace()} method and a default information
     * handler that does nothing.
     *
     * @param threadPoolSize size of thread pool used for polling file system
     *            for
     *            changes
     * @param period interval of time between each poll
     * @param unit time unit of {@code period}
     */
    public PollingWatchService(int threadPoolSize, long period, TimeUnit unit) {
        this(threadPoolSize, period, unit, null, null);
    }

    /**
     * Constructs a new service with the specified thread pool size, period,
     * time
     * unit, exception handler, and information handler.
     *
     * @param threadPoolSize size of thread pool used for polling file system
     *            for
     *            changes
     * @param period interval of time between each poll
     * @param unit time unit of {@code period}
     * @param handler exception handler to handle exceptions thrown in this
     *            service; if {@code null}, a default handler will be used that
     *            invokes the exception's {@code printStackTrace()} method
     * @param infoHandler information handler to handle information published by
     *            this service; if {@code null}, a default handler will be used
     *            that does nothing
     */
    public PollingWatchService(int threadPoolSize, long period, TimeUnit unit,
            PollingWatchServiceExceptionHandler handler,
            PollingWatchServiceInfoHandler infoHandler) {
        super();
        this.period = period;
        this.periodUnit = unit;
        useFileMetadata = true;
        started = false;
        queue = new LinkedBlockingQueue<>();
        keys = new ConcurrentHashMap<>(16, 0.75f, threadPoolSize);
        executor = Executors.newFixedThreadPool(threadPoolSize,
                new ThreadFactoryBuilder()
                        .setNameFormat("polling-watch-service-executor")
                        .setDaemon(true).build());
        timer = Executors
                .newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                        .setNameFormat("polling-watch-service-timer")
                        .setDaemon(true).build());
        this.exceptionHandler = (handler != null) ? handler
                : newDefaultExceptionHandler();
        this.infoHandler = (infoHandler != null) ? infoHandler
                : newDefaultInfoHandler();
        registerLock = new Object();
        closedSentinel = new PollingWatchKey(this, Paths.get(""));
        closeLock = new Object();
        closed = false;
        registerMethodEverInvoked = false;
    }

    /**
     * Answers whether this service is configured to use file metadata. The
     * default is {@code true}.
     *
     * @return {@code true} if this service is configured to use file metadata;
     *         {@code false} otherwise
     *
     * @see #useFileMetadata(boolean)
     */
    public boolean useFileMetadata() {
        return useFileMetadata;
    }

    /**
     * Configures this service to use file metadata or not.
     * <p>
     * When using file metadata, this service will read metadata (i.e.
     * {@link BasicFileAttributes}) for all files in directories being monitored
     * when polling for changes, and it will use the metadata to determine which
     * events need to be signaled. When not using file metadata, the files in
     * directories being monitored will be listed when polling for changes, but
     * the metadata for those files will not be read. The {@code ENTRY_CREATE},
     * {@code ENTRY_DELETE}, and {@code OVERFLOW} watch event kinds may be
     * signaled, but the {@code ENTRY_MODIFY} event kind will never be signaled
     * because the metadata that would enable the detection of a modification is
     * not read.
     * <p>
     * On some file systems, reading file metadata may be expensive. Configuring
     * this service to not use file metadata provides a way to not incur the
     * cost
     * of reading file metadata when polling for changes, but gives up the
     * {@code ENTRY_MODIFY} event kind. A consumer of the events produced by
     * this
     * service when configured to not use file metadata would be able to detect
     * file creation and deletion, but would not, for example, be able to detect
     * that a file was modified.
     * <p>
     * If this method is to be invoked, it must be invoked before any paths are
     * registered via the {@code register} methods of this service and before
     * this service is started.
     *
     * @param useFileMetadata {@code true} to use file metadata when determining
     *            which events to signal; {@code false} to not
     *
     * @throws IllegalStateException if a {@code register} method has already
     *             been invoked or this service has already been started
     *
     * @see #useFileMetadata()
     */
    public void useFileMetadata(boolean useFileMetadata) {
        if(registerMethodEverInvoked) {
            throw new IllegalStateException("Register method already invoked");
        }
        synchronized (closeLock) {
            if(started)
                throw new IllegalStateException("Service already started");
        }
        this.useFileMetadata = useFileMetadata;
    }

    /**
     * @throws ClosedWatchServiceException {@inheritDoc}
     * @throws IllegalStateException {@inheritDoc}
     */
    @Override
    public void start() {
        synchronized (closeLock) {
            if(closed)
                throw new ClosedWatchServiceException();
            if(started)
                throw new IllegalStateException("Already started");
            started = true;
            timer.scheduleAtFixedRate(newPollForChangesRunnable(this), 0L,
                    period, periodUnit);
        }
    }

    /**
     * Note: runs in a single separate thread.
     */
    private void pollForChanges() throws InterruptedException {
        try {
            long startTime = System.nanoTime();
            List<Future<?>> f = new ArrayList<>(keys.size());
            for (PollingWatchKey each : keys.values()) {
                f.add(executor
                        .submit(newPollForChangesOnKeyCallable(this, each)));
            }
            for (Future<?> each : f) {
                try {
                    each.get();
                }
                catch (CancellationException e) {
                    /* IGNORED */
                }
                catch (ExecutionException e) {
                    exceptionHandler.exception(e);
                }
            }
            long endTime = System.nanoTime();
            infoHandler.pollTime(endTime - startTime);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        }
        catch (ThreadDeath td) {
            throw td;
        }
        catch (Throwable t) {
            exceptionHandler.exception(t);
        }
    }

    /**
     * Note: runs in multiple separate threads.
     */
    private void pollForChanges(PollingWatchKey key) throws IOException {
        Path dir = key.watchablePath();
        if(key.isInvalid()) {
            keys.remove(dir);
            return;
        }
        if(!Files.isDirectory(dir, LinkOption.NOFOLLOW_LINKS)) {
            key.cancel();
            keys.remove(dir);
            key.enqueue();
            return;
        }
        Map<Path, BasicFileAttributes> oldEntries = new HashMap<>(
                key.entries());
        Map<Path, BasicFileAttributes> newEntries = useFileMetadata
                ? readDirectoryEntries(dir)
                : readDirectoryEntriesWithoutMetadata(dir, oldEntries);
        for (Path each : newEntries.keySet()) {
            BasicFileAttributes oldEntryAttributes = oldEntries.remove(each);
            if(oldEntryAttributes == null) {
                if(key.hasKind(StandardWatchEventKinds.ENTRY_CREATE)) {
                    key.addEvent(PollingWatchEvent.create(each));
                }
            }
            else {
                if(key.hasKind(StandardWatchEventKinds.ENTRY_MODIFY)) {
                    BasicFileAttributes newEntryAttributes = newEntries
                            .get(each);
                    if(!equals(oldEntryAttributes, newEntryAttributes)) {
                        key.addEvent(PollingWatchEvent.modify(each));
                    }
                }
            }
        }
        if(key.hasKind(StandardWatchEventKinds.ENTRY_DELETE)) {
            for (Path each : oldEntries.keySet()) {
                key.addEvent(PollingWatchEvent.delete(each));
            }
        }
        key.entries(newEntries);
        key.enqueueIfReadyAndHasEvents();
    }

    @Override
    public void close() {
        synchronized (closeLock) {
            if(closed)
                return;
            closed = true;
            Throwable t = null;
            boolean interrupted = Thread.interrupted();
            for (ExecutorService each : Arrays.asList(timer, executor)) {
                try {
                    each.shutdownNow();
                    for (;;) {
                        try {
                            each.awaitTermination(Long.MAX_VALUE,
                                    TimeUnit.DAYS);
                            break;
                        }
                        catch (InterruptedException e) {
                            interrupted = true;
                        }
                    }
                }
                catch (ThreadDeath td) {
                    t = td;
                }
                catch (Throwable th) {
                    if(t == null) {
                        t = th;
                    }
                    else {
                        t.addSuppressed(th);
                    }
                }
            }
            for (PollingWatchKey each : keys.values()) {
                try {
                    each.cancel();
                }
                catch (ThreadDeath td) {
                    t = td;
                }
                catch (Throwable th) {
                    if(t == null) {
                        t = th;
                    }
                    else {
                        t.addSuppressed(th);
                    }
                }
            }
            try {
                keys.clear();
            }
            catch (ThreadDeath td) {
                t = td;
            }
            catch (Throwable th) {
                if(t == null) {
                    t = th;
                }
                else {
                    t.addSuppressed(th);
                }
            }
            try {
                queue.clear();
                queue.add(closedSentinel);
            }
            catch (ThreadDeath td) {
                t = td;
            }
            catch (Throwable th) {
                if(t == null) {
                    t = th;
                }
                else {
                    t.addSuppressed(th);
                }
            }
            if(interrupted)
                Thread.currentThread().interrupt();
            if(t != null)
                throwUncheckedException(t);
        }
    }

    /**
     * @throws ClosedWatchServiceException {@inheritDoc}
     */
    @Override
    public WatchKey poll() {
        if(closed)
            throw new ClosedWatchServiceException();
        PollingWatchKey result = queue.poll();
        if(result == closedSentinel)
            throw new ClosedWatchServiceException();
        return result;
    }

    /**
     * @throws ClosedWatchServiceException {@inheritDoc}
     */
    @Override
    public WatchKey poll(long timeout, TimeUnit unit)
            throws InterruptedException {
        if(closed)
            throw new ClosedWatchServiceException();
        PollingWatchKey result = queue.poll(timeout, unit);
        if(result == closedSentinel)
            throw new ClosedWatchServiceException();
        return result;
    }

    /**
     * @throws ClosedWatchServiceException {@inheritDoc}
     */
    @Override
    public WatchKey take() throws InterruptedException {
        if(closed)
            throw new ClosedWatchServiceException();
        PollingWatchKey result = queue.take();
        if(result == closedSentinel)
            throw new ClosedWatchServiceException();
        return result;
    }

    /**
     * @throws ClosedWatchServiceException {@inheritDoc}
     */
    @Override
    public WatchKey register(Path dir, WatchEvent.Kind<?>... kinds)
            throws IOException {
        return register(dir, kinds, new WatchEvent.Modifier[0]);
    }

    /**
     * @throws ClosedWatchServiceException {@inheritDoc}
     */
    @Override
    public WatchKey register(Path dir, WatchEvent.Kind<?>[] kinds,
            WatchEvent.Modifier... modifiers) throws IOException {
        registerMethodEverInvoked = true;
        /*
         * Perform some sanity checks and read the directory entries before
         * acquiring registerLock so that we aren't holding the lock while
         * performing the potentially time consuming directory entries read.
         * This
         * means that for the common case of a new directory registration, we
         * get
         * good concurrency. For the uncommon case of registering a directory
         * that
         * is already registered, we will perform a directory entries read
         * unnecessarily since it will not be used (if the key is still valid).
         */
        if(closed)
            throw new ClosedWatchServiceException();
        if(!Files.isDirectory(dir)) {
            throw new NotDirectoryException(dir.toString());
        }
        Map<Path, BasicFileAttributes> dirEntries = useFileMetadata
                ? readDirectoryEntries(dir)
                : readDirectoryEntriesWithoutMetadata(dir);
        /*
         * It is possible for an invalid key to be returned, but this is by
         * design
         * and does not violate the contract specified by Path.register nor
         * Watchable.register.
         */
        synchronized (registerLock) {
            if(closed)
                throw new ClosedWatchServiceException();
            if(!Files.isDirectory(dir)) {
                throw new NotDirectoryException(dir.toString());
            }
            PollingWatchKey k = keys.get(dir);
            if(k != null && k.isInvalid()) {
                keys.remove(dir);
                k = null;
            }
            if(k == null) {
                k = new PollingWatchKey(this, dir, kinds, modifiers,
                        dirEntries);
                synchronized (closeLock) {
                    if(closed) {
                        throw new ClosedWatchServiceException();
                    }
                    else {
                        keys.put(dir, k);
                    }
                }
                return k;
            }
            else {
                k.kindsAndModifiers(kinds, modifiers);
                return k;
            }
        }
    }

    void enqueue(PollingWatchKey k) {
        queue.add(k);
    }

    private static PollingWatchServiceExceptionHandler newDefaultExceptionHandler() {
        return new PollingWatchServiceExceptionHandler() {
            @Override
            public void exception(Throwable t) {
                t.printStackTrace();
            }
        };
    }

    private static PollingWatchServiceInfoHandler newDefaultInfoHandler() {
        return new PollingWatchServiceInfoHandler() {
            @Override
            public void pollTime(long t) {
                /* IGNORED */
            }
        };
    }

    private static Runnable newPollForChangesRunnable(
            final PollingWatchService s) {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    s.pollForChanges();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };
    }

    private static Callable<?> newPollForChangesOnKeyCallable(
            final PollingWatchService s, final PollingWatchKey k) {
        return new Callable<Void>() {
            @Override
            public Void call() throws IOException {
                s.pollForChanges(k);
                return null;
            }
        };
    }

    private static void throwUncheckedException(Throwable t) {
        if(t instanceof RuntimeException)
            throw (RuntimeException) t;
        if(t instanceof Error)
            throw (Error) t;
        throw new RuntimeException(t);
    }

    private static boolean equals(BasicFileAttributes a,
            BasicFileAttributes b) {
        return JavaUtilities.equalsIgnoreLastAccessTime(a, b);
    }

    private static Map<Path, BasicFileAttributes> readDirectoryEntries(
            Path dir) {
        Map<Path, BasicFileAttributes> result = new HashMap<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (Path each : stream) {
                try {
                    result.put(each.getFileName(), readAttributes(each));
                }
                catch (IOException e) {
                    /*
                     * IGNORED: The entry may have been deleted, or a number of
                     * other
                     * problems may have occurred. Just read what we can.
                     */
                }
            }
        }
        catch (DirectoryIteratorException | IOException e) {
            /*
             * IGNORED: Various problems could occur while creating a new
             * directory
             * stream or reading the directory. Just return what we have read so
             * far.
             */
            return result;
        }
        return result;
    }

    private static BasicFileAttributes readAttributes(Path p)
            throws IOException {
        return Files.readAttributes(p, BasicFileAttributes.class,
                LinkOption.NOFOLLOW_LINKS);
    }

    private static Map<Path, BasicFileAttributes> readDirectoryEntriesWithoutMetadata(
            Path dir) {
        return readDirectoryEntriesWithoutMetadata(dir,
                Collections.<Path, BasicFileAttributes> emptyMap());
    }

    private static Map<Path, BasicFileAttributes> readDirectoryEntriesWithoutMetadata(
            Path dir, Map<Path, BasicFileAttributes> preexistingMetadata) {
        Map<Path, BasicFileAttributes> result = new HashMap<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            BasicFileAttributes noMetadataAttributes = NoMetadataFileAttributes
                    .instance();
            for (Path each : stream) {
                Path eachFileName = each.getFileName();
                BasicFileAttributes eachAttributes = preexistingMetadata
                        .get(eachFileName);
                result.put(eachFileName, eachAttributes != null ? eachAttributes
                        : noMetadataAttributes);
            }
        }
        catch (DirectoryIteratorException | IOException e) {
            /*
             * IGNORED: Various problems could occur while creating a new
             * directory
             * stream or reading the directory. Just return what we have read so
             * far.
             */
            return result;
        }
        return result;
    }

    private static final class NoMetadataFileAttributes
            implements BasicFileAttributes {
        private static final NoMetadataFileAttributes INSTANCE = new NoMetadataFileAttributes();
        private static final FileTime EPOCH = FileTime.fromMillis(0L);

        private NoMetadataFileAttributes() {}

        @Override
        public FileTime lastModifiedTime() {
            return EPOCH;
        }

        @Override
        public FileTime lastAccessTime() {
            return EPOCH;
        }

        @Override
        public FileTime creationTime() {
            return EPOCH;
        }

        @Override
        public boolean isRegularFile() {
            return false;
        }

        @Override
        public boolean isDirectory() {
            return false;
        }

        @Override
        public boolean isSymbolicLink() {
            return false;
        }

        @Override
        public boolean isOther() {
            return false;
        }

        @Override
        public long size() {
            return 0;
        }

        @Override
        public Object fileKey() {
            return null;
        }

        public static NoMetadataFileAttributes instance() {
            return INSTANCE;
        }
    }
}
