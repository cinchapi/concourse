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
package com.cinchapi.concourse.server.storage;

import static com.google.common.base.Preconditions.checkArgument;

import java.nio.ByteBuffer;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import com.cinchapi.concourse.annotate.Restricted;
import com.cinchapi.concourse.server.concurrent.LockBroker;
import com.cinchapi.concourse.server.concurrent.LockBroker.Permit;
import com.cinchapi.concourse.server.concurrent.LockType;
import com.cinchapi.concourse.server.concurrent.RangeToken;
import com.cinchapi.concourse.server.concurrent.Token;
import com.cinchapi.concourse.server.io.ByteSink;
import com.cinchapi.concourse.server.io.Byteable;
import com.cinchapi.concourse.server.model.Ranges;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.server.storage.temp.Queue;
import com.cinchapi.concourse.server.storage.temp.ToggleQueue;
import com.cinchapi.concourse.server.storage.temp.Write;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.thrift.TObject.Aliases;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Logger;
import com.cinchapi.concourse.util.Transformers;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

/**
 * A linearizable sequence of reads and writes that all succeed or fail
 * together. Each atomic operation is staged in an isolated buffer before being
 * committed to a destination store. For optimal concurrency, we use
 * <em>just in time locking</em> where destination resources are only locked
 * when its time to commit the operation.
 * 
 * @implNote Does not require the use of a {@link ToggleQueue} (and the
 *           associated overhead) because it is assumed that internally defined
 *           {@link AtomicOperation AtomicOperations} won't toggle a
 *           {@link Write} topic.
 * 
 * @author Jeff Nelson
 */
@NotThreadSafe
public class AtomicOperation extends BufferedStore implements
        AtomicSupport,
        TokenEventObserver {

    // NOTE: This class does not need to do any locking on operations (until
    // commit time) because it is assumed to be isolated to one thread and the
    // #source is assumed to have its own concurrency control scheme in
    // place.

    /**
     * Start a new {@link AtomicOperation} that will {@link #commit() commit} to
     * {@code store}.
     * 
     * @param store
     * @param broker
     * @return the AtomicOperation
     */
    protected static AtomicOperation start(AtomicSupport store,
            LockBroker broker) {
        return new AtomicOperation(store, broker);
    }

    /**
     * The initial capacity
     */
    protected static final int INITIAL_CAPACITY = 10;

    /**
     * {@link Status Statuses} that can be
     * {@link #isPreemptedBy(TokenEvent, Token) preempted} by a
     * {@link TokenEvent}.
     */
    private static final Set<Status> PREEMPTIBLE_STATUSES = ImmutableSet
            .of(Status.OPEN, Status.PENDING);

    /**
     * The collection of {@link LockDescription} objects that are grabbed in the
     * {@link #acquireLocks()} method at commit time.
     */
    @Nullable
    protected Set<LockDescription> locks = null;

    /**
     * The {@link LockBroker} that is used to coordinate concurrent operations.
     */
    protected final LockBroker broker;

    /**
     * Whenever a nested {@link AtomicOperation} is
     * {@link #startAtomicOperation() started}, it, by virtue of being a
     * {@link TokenEventObserver}, {@link #subscribe(TokenEventObserver)
     * subscribes} for announcements about token events from this operation.
     * This operation {@link #observe(TokenEvent, Token) observes} on behalf of
     * the child {@link AtomicOperation}, intercepting announcements from its
     * own {@link #source} store (e.g., the {@link Engine}) and
     * {@link AtomicOperation#abort() aborting} the child
     * {@link AtomicOperation} if it
     * {@link AtomicOperation#isPreemptedBy(TokenEvent, Token) concerns} an
     * {@link #observe(TokenEvent, Token) observed} {@link TokenEvent event} for
     * a {@link Token}
     * <p>
     * This collection is non thread-safe because it is assumed that only one
     * nested {@link AtomicOperation} will live at a time, so there will ever
     * only be one {@link #observers observer}.
     * </p>
     */
    private final Set<TokenEventObserver> observers = new HashSet<>(1);

    /**
     * Tracks the {@link Status} of this {@link AtomicOperation}.
     */
    private final AtomicReference<Status> status = new AtomicReference<>(
            Status.OPEN);

    /**
     * The {@link RangeToken range read tokens} that represent any queries in
     * this operation that we must grab locks for at commit time.
     */
    private final RangeHolder rangeReads2Lock = new RangeHolder();

    /**
     * The read {@link Token tokens} that represent any record based reads in
     * this operation that we must grab locks for at commit time.
     */
    private final Set<Token> reads2Lock = new HashSet<>();

    /**
     * A casted pointer to the destination store, which is the source from which
     * this Atomic Operation stems.
     */
    private final AtomicSupport source;

    /**
     * A handler that bypasses the overloaded logic defined in this class and
     * hooks into the "unlocked" logic of {@link BufferedStore} for read
     * methods. This handler facilitates the methods that are defined in
     * {@link LockFreeStore}.
     */
    private final BufferedStore unlocked;

    /**
     * This map contains all the records in which a "wide read" (e.g. a read
     * that touches every field in the record) was performed. This data is used
     * to determine if lock coarsening can be performed at commit time.
     */
    private final Map<Long, Token> wideReads = new HashMap<>();

    /**
     * The write {@link Token tokens} or {@link RangeToken range write tokens}
     * that represent the writes in this operation that we must grab locks for
     * at commit time. Both write and range write tokens are stored in the same
     * collection for efficiency reasons.
     */
    private final Set<Token> writes2Lock = new HashSet<>();

    /**
     * The {@link Tokens} for which locks must be grabbed, but don't cause any
     * conflicts that necessitate {@link Status#PREEMPTED preemption} if
     * there is a version change. These {@link Token} are usually shared and
     * wide write {@link Token tokens}.
     */
    private final Set<Token> exemptions = new HashSet<>();

    /**
     * A queue that holds {@link Token tokens} for which
     * {@link TokenEvent#VERSION_CHANGE version changes} are announced but are
     * not immediately assessed for preemption.
     */
    private final java.util.Queue<Token> queued = new ConcurrentLinkedQueue<>();

    /**
     * Construct a new instance.
     * 
     * @param destination
     */
    protected AtomicOperation(AtomicSupport destination, LockBroker broker) {
        this(new Queue(INITIAL_CAPACITY), destination, broker);
    }

    /**
     * Construct a new instance.
     * 
     * @param buffer
     * @param destination
     * @param lockService
     * @param rangeLockService
     */
    protected AtomicOperation(Queue buffer, AtomicSupport destination,
            LockBroker broker) {
        super(buffer, destination);
        this.broker = broker;
        this.source = (AtomicSupport) this.durable;
        this.unlocked = new BufferedStore(limbo, durable) {

            @Override
            public void start() {}

            @Override
            public void stop() {}

            @Override
            protected boolean verifyWithReentrancy(Write write) {
                return super.verify(write);
            }

        };
        source.subscribe(this);
    }

    /**
     * Close this operation and release all of the held locks without applying
     * any of the changes to the {@link #source} store.
     */
    public void abort() {
        if(status.compareAndSet(Status.OPEN, Status.FINALIZING)
                || status.compareAndSet(Status.PENDING, Status.FINALIZING)
                || status.compareAndSet(Status.PREEMPTED, Status.FINALIZING)) {
            source.unsubscribe(this);
            if(locks != null && !locks.isEmpty()) {
                releaseLocks();
            }
            status.compareAndSet(Status.FINALIZING, Status.ABORTED);
        }
        else if(status.get() != Status.ABORTED) {
            throw new IllegalStateException(
                    "Cannot abort from status: " + status);
        }
    }

    @Override
    public void accept(Write write) {
        // Accept writes from an a nested AtomicOperation and put them in this
        // operation's buffer without performing an additional #verify, but
        // grabbing the necessary lock intentions.
        checkArgument(write.getType() != Action.COMPARE);
        if(write.getType() == Action.ADD) {
            add(write, Sync.NO, Verify.NO);
        }
        else {
            remove(write, Sync.NO, Verify.NO);
        }
    }

    @Override
    public void accept(Write write, boolean sync) {
        accept(write);
    }

    @Override
    public final boolean add(String key, TObject value, long record)
            throws AtomicStateException {
        return add(Write.add(key, value, record), Sync.NO, Verify.YES);
    }

    @Override
    public void announce(TokenEvent event, Token... tokens) {}

    @Override
    public final Map<TObject, Set<Long>> browse(String key)
            throws AtomicStateException {
        checkState();
        Text key0 = Text.wrapCached(key);
        RangeToken rangeToken = RangeToken.forReading(key0, Operator.BETWEEN,
                Value.NEGATIVE_INFINITY, Value.POSITIVE_INFINITY);
        Iterable<Range<Value>> ranges = rangeToken.ranges();
        for (Range<Value> range : ranges) {
            rangeReads2Lock.put(key0, range);
        }
        return super.browse(key);
    }

    @Override
    public final Map<TObject, Set<Long>> browse(String key, long timestamp)
            throws AtomicStateException {
        if(timestamp > Time.now()) {
            return browse(key);
        }
        else {
            checkState();
            return super.browse(key, timestamp);
        }
    }

    @Override
    public Map<TObject, Set<Long>> browseUnlocked(String key) {
        return unlocked.browse(key);
    }

    @Override
    public final Map<Long, Set<TObject>> chronologize(String key, long record,
            long start, long end) throws AtomicStateException {
        checkState();
        long now = Time.now();
        if(start > now || end > now) {
            // Must perform a locking read to prevent a non-repeatable read if
            // writes occur between the present and the future timestamp(s)
            Token token = Token.wrap(key, record);
            reads2Lock.add(token);
            return super.chronologize(key, record, start, end);
        }
        else {
            return super.chronologize(key, record, start, end);
        }
    }

    @Override
    public Map<Long, Set<TObject>> chronologizeUnlocked(String key, long record,
            long start, long end) {
        return unlocked.chronologize(key, record, start, end);
    }

    /**
     * Commit the operation and apply its effects to the {@link #source}.
     * <p>
     * This method only returns {@code true} if all the effects can be
     * successfully applied as a unit. If the commit fails, non of the effects
     * are applied and the caller may retry the operation.
     * </p>
     * <p>
     * The two-phase commit protocol is used:
     * <ul>
     * <li>
     * First, the operation {@link #prepare() makes preparations} to commit and
     * confirms whether it can guarantee successful {@link #complete(long)
     * completion}.
     * </li>
     * <li>
     * If the operation confirms it can {@link #complete(long) complete}, it
     * proceeds.
     * </li>
     * <li>
     * Otherwise, the operation is {@link #cancel() cancelled}.
     * </li>
     * </ul>
     * </p>
     * 
     * @param version the {@link Versioned#getVersion() version} to apply to all
     *            the writes in this {@link AtomicOperation}
     * @return {@code true} if the effects of the operation are completely
     *         applied
     */
    @VisibleForTesting
    public final boolean commit(long version) throws AtomicStateException {
        if(prepare()) {
            complete(version);
            return true;
        }
        else {
            cancel();
            return false;
        }
    }

    @Override
    public final boolean contains(long record) {
        checkState();
        Token token = Token.wrap(record);
        reads2Lock.add(token);
        wideReads.put(record, token);
        return super.contains(record);
    }

    @Override
    public Map<Long, Set<TObject>> explore(String key, Aliases aliases) {
        checkState();
        Operator operator = aliases.operator();
        TObject[] values = aliases.values();
        Text key0 = Text.wrapCached(key);
        RangeToken rangeToken = RangeToken.forReading(key0, operator,
                Transformers.transformArray(values, Value::wrap, Value.class));
        Iterable<Range<Value>> ranges = rangeToken.ranges();
        for (Range<Value> range : ranges) {
            rangeReads2Lock.put(key0, range);
        }
        return super.explore(key, aliases);
    }

    @Override
    public Map<Long, Set<TObject>> explore(String key, Aliases aliases,
            long timestamp) {
        if(timestamp > Time.now()) {
            return explore(key, aliases);
        }
        else {
            checkState();
            return super.explore(key, aliases, timestamp);
        }
    }

    @Override
    public Map<Long, Set<TObject>> exploreUnlocked(String key,
            Aliases aliases) {
        return unlocked.explore(key, aliases);
    }

    @Override
    public final Set<TObject> gather(String key, long record)
            throws AtomicStateException {
        checkState();
        Token token = Token.wrap(key, record);
        reads2Lock.add(token);
        return super.gather(key, record);
    }

    @Override
    public final Set<TObject> gather(String key, long record, long timestamp)
            throws AtomicStateException {
        if(timestamp > Time.now()) {
            return gather(key, record);
        }
        else {
            checkState();
            return super.gather(key, record, timestamp);
        }
    }

    @Override
    public Set<TObject> gatherUnlocked(String key, long record) {
        return unlocked.gather(key, record);
    }

    @Override
    @Restricted
    public boolean observe(TokenEvent event, Token token) {
        while (observers == null) {
            // Account for a race condition where the Transaction (via
            // AtomicOperation) subscribes for TokenEvents before the #observers
            // collection is set during Transaction construction
            Logger.warn("An atomic operation handled by {} received a Token "
                    + "Event announcement before it was fully initialized",
                    Thread.currentThread());
            Thread.yield();
        }
        boolean intercepted = false;
        try {
            Iterator<TokenEventObserver> it = observers.iterator();
            while (it.hasNext()) {
                TokenEventObserver observer = it.next();
                AtomicOperation atomic = (AtomicOperation) observer;
                if(atomic.isPreemptedBy(event, token)) {
                    atomic.abort();
                    intercepted = true;
                    it.remove();
                }
            }
            if(intercepted) {
                return true;
            }
            else {
                // NOTE: If the AtomicOperation is preempted by #event, an
                // explicit call to source.unsubscribe() isn't made here,
                // because the Engine will automatically remove this
                // AtomicOperation from its list of known observers.
                return isImmediatelyPreemptedBy(event, token)
                        && (status.compareAndSet(Status.OPEN, Status.PREEMPTED)
                                || status.compareAndSet(Status.PENDING,
                                        Status.PREEMPTED));
            }
        }
        catch (ConcurrentModificationException e) {
            // Another asynchronous write or announcement was received while
            // observing the token event, so a retry is necessary.
            return observe(event, token);
        }
    }

    @Override
    public void onCommit(AtomicOperation operation) {
        absorb(operation);
    }

    @Override
    public final boolean remove(String key, TObject value, long record)
            throws AtomicStateException {
        return remove(Write.remove(key, value, record), Sync.NO, Verify.YES);
    }

    @Override
    public final void repair() {/* no-op */}

    @Override
    public final Map<Long, List<String>> review(long record)
            throws AtomicStateException {
        checkState();
        Token token = Token.wrap(record);
        reads2Lock.add(token);
        wideReads.put(record, token);
        return super.review(record);
    }

    @Override
    public final Map<Long, List<String>> review(String key, long record)
            throws AtomicStateException {
        checkState();
        Token token = Token.wrap(key, record);
        reads2Lock.add(token);
        return super.review(key, record);
    }

    @Override
    public Map<Long, List<String>> reviewUnlocked(long record) {
        return unlocked.review(record);
    }

    @Override
    public Map<Long, List<String>> reviewUnlocked(String key, long record) {
        return unlocked.review(key, record);
    }

    @Override
    public final Set<Long> search(String key, String query)
            throws AtomicStateException {
        checkState();
        return super.search(key, query);
    }

    @Override
    public final Map<String, Set<TObject>> select(long record)
            throws AtomicStateException {
        checkState();
        Token token = Token.wrap(record);
        reads2Lock.add(token);
        wideReads.put(record, token);
        return super.select(record);
    }

    @Override
    public final Map<String, Set<TObject>> select(long record, long timestamp)
            throws AtomicStateException {
        if(timestamp > Time.now()) {
            return select(record);
        }
        else {
            checkState();
            return super.select(record, timestamp);
        }
    }

    @Override
    public final Set<TObject> select(String key, long record)
            throws AtomicStateException {
        checkState();
        Token token = Token.wrap(key, record);
        reads2Lock.add(token);
        return super.select(key, record);
    }

    @Override
    public final Set<TObject> select(String key, long record, long timestamp)
            throws AtomicStateException {
        if(timestamp > Time.now()) {
            return select(key, record);
        }
        else {
            checkState();
            return super.select(key, record, timestamp);
        }
    }

    @Override
    public Map<String, Set<TObject>> selectUnlocked(long record) {
        return unlocked.select(record);
    }

    @Override
    public Set<TObject> selectUnlocked(String key, long record) {
        return unlocked.select(key, record);
    }

    @Override
    public final void set(String key, TObject value, long record)
            throws AtomicStateException {
        checkState();
        Token token = Token.wrap(key, record);
        RangeToken rangeToken = RangeToken.forWriting(Text.wrapCached(key),
                Value.wrap(value));
        Token wide = wideReads.get(record);
        if(wide != null) {
            writes2Lock.add(wide);
        }
        else {
            writes2Lock.add(token);
            // CON-669: Prevent a conflicting wide read, but don't listen for
            // wide version change
            Token shared = Token.shareable(record);
            writes2Lock.add(shared);
            exemptions.add(shared);
        }
        writes2Lock.add(rangeToken);
        super.set(key, value, record);
    }

    @Override
    public final void start() {}

    @Override
    public AtomicOperation startAtomicOperation() {
        checkState();
        /*
         * This operation must adhere to the JIT locking guarantees of its
         * #source. So, when starting a nested operation, this one inherits the
         * lock intentions of its child, but defers locking on behalf of the
         * child until it is ready to commit. As a result, we do not pass the
         * #source's lock broker to the nested operation.
         */
        return AtomicOperation.start(this, LockBroker.noOp());
    }

    /**
     * Return the {@link Status} of this {@link AtomicOperation}.
     * 
     * @return the {@link Status}.
     */
    public final Status status() {
        try {
            checkIfQueuedPreempted();
        }
        catch (AtomicStateException e) {}
        return status.get();
    }

    @Override
    public final void stop() {}

    @Override
    public void subscribe(TokenEventObserver observer) {
        observers.add(observer);
    }

    @Override
    public void sync() {/* no-op */}

    /**
     * Register interest in {@code record} so that this AtomicOperation can
     * listen for changes and grab a read lock at commit time.
     * 
     * @param record
     */
    public void touch(long record) {
        checkState();
        Token token = Token.wrap(record);
        reads2Lock.add(token);
        wideReads.put(record, token);
    }

    @Override
    public void unsubscribe(TokenEventObserver observer) {
        observers.remove(observer);
    }

    @Override
    public final boolean verify(String key, TObject value, long record,
            long timestamp) throws AtomicStateException {
        if(timestamp > Time.now()) {
            return verify(key, value, record);
        }
        else {
            checkState();
            return super.verify(key, value, record, timestamp);
        }
    }

    @Override
    public final boolean verify(Write write) throws AtomicStateException {
        checkState();
        Token token = Token.wrap(write.getKey().toString(),
                write.getRecord().longValue());
        reads2Lock.add(token);
        return super.verify(write);
    }

    @Override
    public boolean verifyUnlocked(String key, TObject value, long record) {
        return unlocked.verify(key, value, record);
    }

    @Override
    public boolean verifyUnlocked(Write write) {
        return unlocked.verify(write);
    }

    @Override
    protected Map<TObject, Set<Long>> $browse(String key) {
        return source.browseUnlocked(key);
    }

    @Override
    protected Map<Long, Set<TObject>> $chronologize(String key, long record,
            long start, long end) {
        return source.chronologizeUnlocked(key, record, start, end);
    }

    @Override
    protected Map<Long, Set<TObject>> $explore(String key, Aliases aliases) {
        return source.exploreUnlocked(key, aliases);
    }

    @Override
    protected Set<TObject> $gather(String key, long record) {
        return source.gatherUnlocked(key, record);
    }

    @Override
    protected Map<Long, List<String>> $review(long record) {
        return source.reviewUnlocked(record);
    }

    @Override
    protected Map<Long, List<String>> $review(String key, long record) {
        return source.reviewUnlocked(key, record);
    }

    @Override
    protected Map<String, Set<TObject>> $select(long record) {
        return source.selectUnlocked(record);
    }

    @Override
    protected Set<TObject> $select(String key, long record) {
        return source.selectUnlocked(key, record);
    }

    @Override
    protected boolean $verify(Write write) {
        return source.verifyUnlocked(write);
    }

    @Restricted
    protected void absorb(AtomicOperation atomic) {
        if(atomic.source == this) {
            if(atomic.status.get() == Status.FINALIZING) {
                rangeReads2Lock.ranges.putAll(atomic.rangeReads2Lock.ranges);
                reads2Lock.addAll(atomic.reads2Lock);
                wideReads.putAll(atomic.wideReads);
                writes2Lock.addAll(atomic.writes2Lock);
                exemptions.addAll(atomic.exemptions);
            }
            else {
                throw new IllegalStateException(
                        "Cannot absorb an atomic operation that is not finalizing");
            }
        }
        else {
            throw new IllegalStateException(
                    "Cannot absorb an atomic operation with a different source");
        }
    }

    @Override
    protected final boolean add(Write write, Sync sync, Verify verify)
            throws AtomicStateException {
        checkState();
        String key = write.getKey().toString();
        long record = write.getRecord().longValue();
        Token token = Token.wrap(key, record);
        RangeToken rangeToken = RangeToken.forWriting(write.getKey(),
                write.getValue());
        Token wide = wideReads.get(record);
        if(wide != null) {
            writes2Lock.add(wide);
        }
        else {
            writes2Lock.add(token);
            // CON-669: Prevent a conflicting wide read, but don't listen for
            // wide version change
            Token shared = Token.shareable(record);
            writes2Lock.add(shared);
            exemptions.add(shared);
        }
        writes2Lock.add(rangeToken);
        return super.add(write, sync, verify);
    }

    /**
     * Transport the written data to the {@link #durable} store. The
     * subclass may override this method to do additional things (i.e. backup
     * the data, etc) if necessary.
     */
    protected void apply() {
        apply(false);
    }

    /**
     * Transport the written data to the {@link #durable} store. The
     * subclass may override this method to do additional things (i.e. backup
     * the data, etc) if necessary.
     * 
     * @param syncAndVerify a flag that controls whether this operation will
     *            cause the {@code destination} to always perform a sync and
     *            verify for each write that is transported. If this value is
     *            set to {@code false}, this operation will transport all the
     *            writes without instructing the destination to sync and verify.
     *            Once all the writes have been transported, the destination
     *            will be instructed to sync the writes as a group (GROUP SYNC),
     *            but no verification will occur for any of the writes (which is
     *            okay as long as this operation implicitly verifies each write
     *            prior to commit, see CON-246).
     *            <p>
     *            NOTE: This parameter is eventually passed from the
     *            {@code verify} parameter in a call to
     *            {@link BufferedStore#add(String, TObject, long, boolean, boolean, boolean)}
     *            or
     *            {@link BufferedStore#remove(String, TObject, long, boolean, boolean, boolean)}
     *            . Generally speaking, this coupling between optional syncing
     *            and optional verifying is okay because it doesn't make sense
     *            to sync but not verify or verify but not sync.
     *            </p>
     */
    protected void apply(boolean syncAndVerify) {
        // Since we don't take a backup, it is possible that we can end up
        // in a situation where the server crashes in the middle of the data
        // transport, which means that the atomic operation would be partially
        // committed on server restart, which would appear to violate the
        // "all or nothing" guarantee. We are willing to live with that risk
        // because the occurrence of that happening seems low and atomic
        // operations don't guarantee consistency or durability, so it is
        // technically not a violation of "all or nothing" if the entire
        // operation succeeds but isn't durable on crash and leaves the database
        // in an inconsistent state.
        limbo.transport(durable, syncAndVerify);
        if(!syncAndVerify) {
            durable.sync();
        }
    }

    /**
     * Check if this operation is preempted by any {@link #queued} version
     * change announcements.
     * <p>
     * If the operation is preempted, an {@link AtomicStateException} is thrown.
     * </p>
     */
    protected void checkIfQueuedPreempted() {
        Token token;
        while ((token = queued.poll()) != null) {
            if(isPreemptedBy(TokenEvent.VERSION_CHANGE, token)
                    && (status.compareAndSet(Status.OPEN, Status.PREEMPTED)
                            || status.compareAndSet(Status.PENDING,
                                    Status.PREEMPTED))) {
                source.unsubscribe(this);
                throwAtomicStateException();
            }
        }
    }

    /**
     * Check that this AtomicOperation is open and throw an
     * AtomicStateException if it is not.
     * 
     * @throws AtomicStateException
     */
    protected void checkState() throws AtomicStateException {
        if(status.get() == Status.PREEMPTED) {
            abort();
        }
        if(status.get() != Status.OPEN) {
            throwAtomicStateException();
        }
        checkIfQueuedPreempted();
    }

    /**
     * Return {@code true} if {@code event} for {@code token} preempts this
     * {@link AtomicOperation operation}.
     * 
     * @param event
     * @param token
     * 
     * @return {@code true} if this {@link AtomicOperation} is
     *         {@link Status#PREEMPTED interrupted} when
     *         {@link #observe(TokenEvent, Token) observing} an
     *         announcement of {@code event} for {@code token}
     */
    @Restricted
    protected boolean isPreemptedBy(TokenEvent event, Token token) {
        for (TokenEventObserver observer : observers) {
            if(((AtomicOperation) observer).isPreemptedBy(event, token)) {
                return true;
            }
        }
        if(event == TokenEvent.VERSION_CHANGE && isPreemptible()) {
            if(token instanceof RangeToken) {
                // NOTE: RangeTokens intended for writes (held in
                // writes2Lock) should never cause the AtomicOperation to be
                // preempted because they are infinitely wide.
                RangeToken rangeToken = (RangeToken) token;
                RangeSet<Value> covered = rangeReads2Lock.ranges
                        .get(rangeToken.getKey());
                if(covered != null) {
                    Iterable<Range<Value>> ranges = rangeToken.ranges();
                    for (Range<Value> range : ranges) {
                        if(!covered.subRangeSet(range).isEmpty()) {
                            return true;
                        }
                    }
                }
            }
            else if((reads2Lock.contains(token) || writes2Lock.contains(token))
                    && !exemptions.contains(token)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Return {@code true} if this Atomic Operation has 0 writes.
     * 
     * @return {@code true} if this atomic operation is considered
     *         <em>read-only</em>
     */
    protected boolean isReadOnly() {
        return ((Queue) limbo).size() == 0;
    }

    @Override
    protected final boolean remove(Write write, Sync sync, Verify verify)
            throws AtomicStateException {
        checkState();
        String key = write.getKey().toString();
        long record = write.getRecord().longValue();
        Token token = Token.wrap(key, record);
        RangeToken rangeToken = RangeToken.forWriting(write.getKey(),
                write.getValue());
        Token wide = wideReads.get(record);
        if(wide != null) {
            writes2Lock.add(wide);
        }
        else {
            writes2Lock.add(token);
            // CON-669: Prevent a conflicting wide read, but don't listen for
            // wide version change
            Token shared = Token.shareable(record);
            writes2Lock.add(shared);
            exemptions.add(shared);
        }
        writes2Lock.add(rangeToken);
        return super.remove(write, sync, verify);
    }

    /**
     * Set the {@link Status} of this {@link AtomicOperation}.
     * 
     * @param status
     */
    protected final void setStatus(Status status) {
        this.status.set(status);
    }

    /**
     * Throw an {@link AtomicStateException}.
     * 
     * @throws AtomicStateException
     */
    protected void throwAtomicStateException() throws AtomicStateException {
        throw new AtomicStateException();
    }

    @Override
    protected boolean verifyWithReentrancy(Write write) {
        return super.verify(write);
    }

    /**
     * Commit the atomic operation to the destination store. The commit is only
     * successful if all the grouped operations can be successfully applied to
     * the destination. If the commit fails, the caller should retry the atomic
     * operation.
     * 
     * @return {@code true} if the atomic operation is completely applied
     */
    @VisibleForTesting
    final boolean commit() throws AtomicStateException {
        return commit(CommitVersions.next());
    }

    /**
     * Check each one of the {@link #intentions} against the
     * {@link #durable} and grab the appropriate locks along the way. This
     * method will return {@code true} if all expectations are met and all
     * necessary locks are grabbed. Otherwise it will return {@code false}, in
     * which case this operation should be aborted immediately.
     * 
     * @return {@code true} if all expectations are met and all necessary locks
     *         are grabbed.
     */
    private boolean acquireLocks() {
        if(isReadOnly()) {
            return true;
        }
        else {
            // NOTE: If we can't grab a lock immediately because it is held by
            // someone else, then we must fail immediately because the
            // AtomicOperation can't properly commit.
            locks = new HashSet<>();
            try {
                // Grab write locks and remove any covered read or range read
                // intentions
                for (Token token : writes2Lock) {
                    if(status.get() == Status.PREEMPTED) {
                        return false;
                    }
                    LockType type;
                    if(token instanceof RangeToken) {
                        RangeToken rangeToken = (RangeToken) token;
                        if(!rangeReads2Lock.isEmpty(rangeToken.getKey())) {
                            Range<Value> containing = rangeReads2Lock.get(
                                    rangeToken.getKey(),
                                    rangeToken.getValues()[0]);
                            if(containing != null) {
                                rangeReads2Lock.remove(rangeToken.getKey(),
                                        containing);
                                Iterable<Range<Value>> xor = Ranges.xor(
                                        Range.singleton(
                                                rangeToken.getValues()[0]),
                                        containing);
                                for (Range<Value> range : xor) {
                                    rangeReads2Lock.put(rangeToken.getKey(),
                                            range);
                                }
                            }
                        }
                        type = LockType.RANGE_WRITE;
                    }
                    else {
                        reads2Lock.remove(token);
                        type = LockType.WRITE;
                    }
                    LockDescription lock = LockDescription.forToken(token,
                            broker, type);
                    if(lock.tryLock()) {
                        locks.add(lock);
                    }
                    else {
                        return false;
                    }
                }
                // Grab the read locks. We can be sure that any remaining
                // intentions are not covered by any of the write locks we
                // grabbed previously.
                for (Token token : reads2Lock) {
                    if(status.get() == Status.PREEMPTED) {
                        return false;
                    }
                    LockDescription lock = LockDescription.forToken(token,
                            broker, LockType.READ);
                    if(lock.tryLock()) {
                        locks.add(lock);
                    }
                    else {
                        return false;
                    }
                }
                // Grab the range read locks. We can be sure that any remaining
                // intentions are not covered by any of the range write locks we
                // grabbed previously.
                for (Entry<Text, RangeSet<Value>> entry : rangeReads2Lock.ranges
                        .entrySet()) { /* (Authorized) */
                    if(status.get() == Status.PREEMPTED) {
                        return false;
                    }
                    Text key = entry.getKey();
                    for (Range<Value> range : entry.getValue().asRanges()) {
                        RangeToken rangeToken = Ranges.convertToRangeToken(key,
                                range);
                        LockDescription lock = LockDescription.forToken(
                                rangeToken, broker, LockType.RANGE_READ);
                        if(lock.tryLock()) {
                            locks.add(lock);
                        }
                        else {
                            return false;
                        }
                    }
                }
            }
            catch (NullPointerException e) {
                // If we are notified a version change while grabbing locks, we
                // abort immediately which means that #locks will become null.
                return false;
            }
            return true;
        }
    }

    /**
     * Cancel the operation and set its status to {@link Status#ABORTED},
     * regardless of its current state.
     */
    private final void cancel() {
        source.unsubscribe(this);
        if(locks != null && !locks.isEmpty()) {
            releaseLocks();
        }
        status.set(Status.ABORTED);
    }

    /**
     * The second phase of the {@link #commit(long) commit} protocol: apply the
     * effects of this operation to the {@link #source}.
     * <p>
     * This method requires that the operation have been {@link #prepare()
     * prepared} so that it is guaranteed that the effects can be applied
     * without conflict.
     * </p>
     * 
     * @param version the {@link Versioned#getVersion() version} to apply to all
     *            the writes in this {@link AtomicOperation}
     */
    private final void complete(long version) {
        if(status.compareAndSet(Status.FINALIZING, Status.FINALIZING)) {
            limbo.transform(write -> write.rewrite(version));
            apply();
            releaseLocks();
            source.onCommit(this);
            if(!status.compareAndSet(Status.FINALIZING, Status.COMMITTED)) {
                throw new IllegalStateException(
                        "Unexpected atomic operation state change");
            }
        }
        else {
            throwAtomicStateException();
        }
    }

    /**
     * Return {@code true} if it can immediately be determined that
     * {@code event} for {@code token} preempts this {@link AtomicOperation
     * operation}.
     * <p>
     * If this method returns {@code false}, it either means that
     * the operation is not preempted by {@code event} for {@code token} or that
     * the determination could not be made immediately. In the latter case, the
     * {@code token} is placed on a {@link #queued queue} and can be later
     * processed using {@link #checkIfQueuedPreempted()}.
     * </p>
     * 
     * @param event
     * @param token
     * @return {@code true} if it is immediately known that {@code event} for
     *         {@code token} preempts this {@link AtomicOperation operation}
     */
    private boolean isImmediatelyPreemptedBy(TokenEvent event, Token token) {
        if(event == TokenEvent.VERSION_CHANGE && token instanceof RangeToken) {
            queued.add(token);
            return false;
        }
        else {
            return isPreemptedBy(event, token);
        }
    }

    /**
     * Return {@code true} if the {@link #status} of this
     * {@link AtomicOperation} means that it can be
     * {@link #isPreemptedBy(TokenEvent, Token) preempted}.
     * 
     * @return {@code true} if this is preemptible
     */
    private boolean isPreemptible() {
        for (Status status : PREEMPTIBLE_STATUSES) {
            if(this.status.compareAndSet(status, status)) {
                return true;
            }
        }
        return false;
    }

    /**
     * The first phase of the {@link #commit(long) commit} protocol: attempt to
     * make all preparations necessary for this operation to guarantee that its
     * effects can be {@link #complete(long) applied}.
     * <p>
     * The operation must be {@link Status#OPEN open}. If this method returns
     * {@code true}, the operation will enter the {@link Status#FINALIZING
     * prepared} state where it is then able to proceed to the final phase of
     * the commit protocol. A return value of {@code true} indicates that all
     * the effects of the operation can be applied as a unit.
     * </p>
     * <p>
     * If, for any reason, this method returns {@code false}, the operation will
     * be in a state where the only option is to {@link #cancel() cancel}.
     * </p>
     * <p>
     * <strong>NOTE:</strong>Attempts to {@link #prepare() prepare} an operation
     * that is not {@link Status#OPEN open} will throw an
     * {@link AtomicStateException}.
     * </p>
     * 
     * @return {@code true} if the transaction can guarantee that its effects
     *         can be {@code #complete(long) applied}
     */
    private final boolean prepare() {
        if(status.compareAndSet(Status.OPEN, Status.PENDING)) {
            checkIfQueuedPreempted();
            if(acquireLocks()) {
                source.unsubscribe(this);
                return status.compareAndSet(Status.PENDING, Status.FINALIZING);
            }
        }
        else {
            try {
                throwAtomicStateException();
            }
            catch (TransactionStateException e) {
                // Thrown by the Transaction subclass to distinguish transaction
                // failures that should be propagated to the client
                throw e;
            }
            catch (AtomicStateException e) {/* ignore */}
        }
        return false;
    }

    /**
     * Release all of the locks that are held by this operation.
     */
    private void releaseLocks() {
        if(isReadOnly()) {
            return;
        }
        else if(locks != null) {
            Set<LockDescription> _locks = locks;
            locks = null; // CON-172: Set the reference of the locks to null
                          // immediately to prevent a race condition where
                          // the #grabLocks method isn't notified of version
                          // change failure in time
            for (LockDescription lock : _locks) {
                lock.unlock(); // We should never encounter an
                               // IllegalMonitorStateException here because a
                               // lock should only go in #locks once it has been
                               // locked.
            }
        }
    }

    /**
     * A LockDescription is a wrapper around a {@link Lock} that contains
     * metadata that can be serialized to disk. The AtomicOperation grabs a
     * collection of LockDescriptions when it goes to commit.
     * 
     * @author Jeff Nelson
     */
    protected static final class LockDescription implements Byteable {

        /**
         * Return the appropriate {@link LockDescription} that will provide
         * coverage for {@code token}
         * 
         * @param token
         * @param broker
         * @param type
         * @return the LockDescription
         */
        public static LockDescription forToken(Token token, LockBroker broker,
                LockType type) {
            return new LockDescription(broker, token, type);
        }

        /**
         * Return the LockDescription encoded in {@code bytes} so long as those
         * bytes adhere to the format specified by the {@link #getBytes()}
         * method. This method assumes that all the bytes in the {@code bytes}
         * belong to the LockDescription. In general, it is necessary to get the
         * appropriate LockDescription slice from the parent ByteBuffer using
         * {@link ByteBuffers#slice(ByteBuffer, int, int)}.
         * 
         * @param bytes
         * @param lockService
         * @param rangeLockService
         * @return the LockDescription
         */
        public static LockDescription fromByteBuffer(ByteBuffer bytes,
                LockBroker broker) {
            LockType type = LockType.values()[bytes.get()];
            Token token = null;
            switch (type) {
            case RANGE_READ:
            case RANGE_WRITE:
                token = RangeToken.fromByteBuffer(bytes);
                break;
            case READ:
            case WRITE:
                token = Token.fromByteBuffer(bytes);
                break;
            }
            return new LockDescription(broker, token, type);
        }

        private final Token token;
        private final LockType type;
        private final LockBroker broker;

        @Nullable
        private transient Permit permit;

        /**
         * Construct a new instance.
         * 
         * @param token
         * @param lock
         * @param type
         */
        private LockDescription(LockBroker broker, Token token, LockType type) {
            this.broker = broker;
            this.type = type;
            this.token = token;
        }

        @Override
        public void copyTo(ByteSink sink) {
            sink.put((byte) type.ordinal());
            token.copyTo(sink);
        }

        @Override
        public boolean equals(Object obj) {
            if(obj instanceof LockDescription) {
                return token.equals(((LockDescription) obj).token);
            }
            else {
                return false;
            }
        }

        /**
         * Return the Token.
         * 
         * @return the token
         */
        public Token getToken() {
            return token;
        }

        /**
         * Return the LockType
         * 
         * @return the LockType
         */
        public LockType getType() {
            return type;
        }

        @Override
        public int hashCode() {
            return token.hashCode();
        }

        @Override
        public int size() {
            return token.size() + 1; // token + type(1)
        }

        /**
         * Try to acquire the lock.
         * 
         * @return {@code true} if the lock could be acquired
         */
        public boolean tryLock() {
            if(type == LockType.READ || type == LockType.RANGE_READ) {
                permit = broker.tryReadLock(token);
            }
            else {
                permit = broker.tryWriteLock(token);
            }
            return permit != null;
        }

        /**
         * Release the lock.
         */
        public void unlock() {
            if(permit != null) {
                permit.release();
            }
            else {
                throw new IllegalMonitorStateException();
            }
        }

    }

    /**
     * The lifecycle states of an {@link AtomicOperation}.
     *
     * @author Jeff
     */
    protected enum Status {
        /**
         * The operation has not {@link #COMMITTED} or {@link #ABORTED} and can
         * service reads and writes.
         */
        OPEN,

        /**
         * The operation has not {@link #COMMITTED} or {@link #ABORTED}, but can
         * no longer service reads and writes.
         */
        PENDING,

        /**
         * The operation has successfully {@link #commit(long) committed) and is
         * no longer {@link #OPEN}.
         */
        COMMITTED,

        /**
         * The operation was automatically or manually {@link #abort() aborted}
         * and is no longer {@link #OPEN}.
         */
        ABORTED,

        /**
         * The operation is in the process of transitioning to either an
         * {@link #COMMITTED} or {@link #ABORTED} state and is no longer
         * {@link #OPEN}.
         * <p>
         * This flag that indicates this atomic operation has successfully
         * grabbed all required locks and is in the process of committing or has
         * been notified of a version change and is in the process of aborting.
         * We use this flag to protect the atomic operation from version change
         * notifications that happen while its committing (because the version
         * change notifications come from the transaction itself).
         * </p>
         */
        FINALIZING,

        /**
         * The operation was preempted and cannot become {@link #COMMITTED}.
         */
        PREEMPTED,

        /**
         * The operation is prepared to become {@link #COMMITTED}.
         */
        PREPARED
    }

    /**
     * Encapsulates the logic to efficiently associate keys with ranges for the
     * purposes of JIT range locking.
     * 
     * @author Jeff Nelson
     */
    private class RangeHolder {

        /**
         * A mapping from each key to the ranges we need to lock for that key.
         */
        final Map<Text, RangeSet<Value>> ranges = Maps.newHashMap(); // accessible
                                                                     // to outer
                                                                     // class

        /**
         * Return the unique range for {@code key} that contains {@code value}
         * or {@code null} if it does not exist.
         * 
         * @param key
         * @param value
         * @return the range containing {@code value} for {@code key}
         */
        public Range<Value> get(Text key, Value value) {
            RangeSet<Value> set = ranges.get(key);
            if(set != null) {
                return set.rangeContaining(value);
            }
            else {
                return null;
            }
        }

        /**
         * Return {@code true} if there are no ranges for {@code key}.
         * 
         * @param key
         * @return {@code true} if the mapped range set is empty
         */
        public boolean isEmpty(Text key) {
            RangeSet<Value> set = ranges.get(key);
            return set == null || set.isEmpty();
        }

        /**
         * Add {@code range} for {@code key}.
         * 
         * @param key
         * @param range
         */
        public void put(Text key, Range<Value> range) {
            RangeSet<Value> set = ranges.get(key);
            if(set == null) {
                set = TreeRangeSet.create();
                ranges.put(key, set);
            }
            set.add(range);
        }

        /**
         * Remove the {@code range} from {@code key}.
         * 
         * @param key
         * @param range
         */
        public void remove(Text key, Range<Value> range) {
            RangeSet<Value> set = ranges.get(key);
            set.remove(range);
            if(set.isEmpty()) {
                ranges.remove(key);
            }
        }

    }

}
