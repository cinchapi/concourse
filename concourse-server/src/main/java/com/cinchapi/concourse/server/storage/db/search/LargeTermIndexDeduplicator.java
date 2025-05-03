/*
 * Copyright (c) 2013-2022 Cinchapi Inc.
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
package com.cinchapi.concourse.server.storage.db.search;

import java.io.Closeable;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.AbstractSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.internal.announcer.InternalAnnouncer;
import net.openhft.chronicle.core.util.ReadResolvable;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.BytesWriter;
import net.openhft.chronicle.map.VanillaChronicleMap;
import net.openhft.chronicle.map.impl.CompiledMapQueryContext;
import net.openhft.chronicle.set.ChronicleSet;

import org.jetbrains.annotations.NotNull;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.common.io.ByteBuffers;
import com.cinchapi.common.logging.Logging;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.annotate.Experimental;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.storage.db.kernel.CorpusChunk.SearchTermMetrics;
import com.cinchapi.concourse.util.FileOps;
import com.cinchapi.concourse.util.Logger;
import com.github.davidmoten.bplustree.BPlusTree;
import com.github.davidmoten.bplustree.LargeByteBuffer;
import com.github.davidmoten.bplustree.Serializer;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

/**
 * An off-heap container that can hold all the unique substrings for a "large"
 * term and therefore be used to ensure that duplicate substrings for a term
 * {code t} at position {@code p} are not indexed (and creating unnecessary
 * bloat).
 *
 * <p>
 * This is used to deduplicate substrings during the
 * {@link com.cinchapi.concourse.server.storage.db.kernel.CorpusChunk#index(Text, Text, com.cinchapi.concourse.server.model.Position, long, com.cinchapi.concourse.server.storage.Action, java.util.Collection)
 * indexing} preparation when holding the substrings of a term in a {@link Set]
 * on heap would likely cause an {@link OutOfMemoryError} if maintained on the
 * heap. As a tradeoff for being more memory efficient, adding to and checking
 * this container for duplicates has slower performance, so it should only be
 * used when it is necessary to avoid fatal memory pressure.
 * </p>
 * <p>
 * <strong>Limitations:</strong>
 * <ul>
 * <li>Because of internal memory optimizations, this container can only used to
 * hold and deduplicate substrings that are represented as {@link Text} objects
 * specifically created by {@link Text#wrap(char[], int, int) wrapping} the
 * term's char[].</li>
 * <li>While this is an implementation of the {@link Set} interface, the only
 * methods that are guaranteed to be support are {@link #contains(Object)} and
 * {@link #add(Text)}. Attempts to iterate the container or perform other kinds
 * of modifications may throw an exception.</li>
 * </ul>
 * </p>
 *
 * @author Jeff Nelson
 */
@NotThreadSafe
@Experimental
public abstract class LargeTermIndexDeduplicator extends AbstractSet<Text>
        implements
        Closeable {

    /**
     * Return an {@link LargeTermIndexDeduplicator} that is configured to
     * accommodate {@code expectedInsertions} number of substrings from
     * {@code term.
     * 
     * @param expectedInsertions
     * 
     * @return the {@link LargeTermIndexDeduplicator}
     */
    public static LargeTermIndexDeduplicator create(char[] term,
            SearchTermMetrics metrics) {
        int expectedInsertions = metrics.upperBoundOfPossibleSubstrings();
        long estimatedMemoryRequired = (long) metrics.averageSubstringLength()
                * (long) expectedInsertions;
        long availableDirectMemory = availableDirectMemory();
        long freeHeapMemory = Runtime.getRuntime().freeMemory();
        Logger.info("The search indexer has encountered a large term that "
                + "may require a lot of memory to process. The term has "
                + "up to {} search indexes{}{}", expectedInsertions,
                System.lineSeparator(), summarizeMemory(estimatedMemoryRequired,
                        availableDirectMemory, freeHeapMemory));

        LargeTermIndexDeduplicator deduplicator;
        if(availableDirectMemory > estimatedMemoryRequired) {
            try {
                Logger.info("Attempting to use off-heap memory to deduplicate "
                        + "the search indexes");
                deduplicator = new ChronicleBackedTermIndexDeduplicator(term,
                        metrics);
            }
            catch (OutOfMemoryError e) {
                Logger.warn("There appeared to be enough off-heap memory to "
                        + "deduplicate up to {} search indexes, but the"
                        + "operating system refused to allocate the requied"
                        + "capacity. As a result, deduplicaton must be performed "
                        + "on disk, which is A LOT slower...",
                        expectedInsertions);
                // This *usually* means that there isn't enough off heap memory
                // needed for Chronicle, so fallback to storing the entries on
                // disk and using a bloom filter to "speed" things up. This will
                // be slow AF, but is a last resort to try to keep things
                // running.
                deduplicator = new BPlusTreeBackedTermIndexDeduplicator(term,
                        metrics);
            }
        }
        else {
            Logger.info("There doesn't appear to be enough off-heap memory to "
                    + "deduplicate up to {} search indexes, so the operation "
                    + "must be performed by manually checking if every single "
                    + "potential index string is a duplicate, which is slower...",
                    expectedInsertions);
            deduplicator = new BruteForceTermIndexDeduplicator(term, metrics);
        }
        return deduplicator;
    }

    /**
     * Return an {@link LargeTermIndexDeduplicator} that is only appropriate for
     * unit tests.
     * 
     * @param term
     * @return the {@link LargeTermIndexDeduplicator}
     */
    static LargeTermIndexDeduplicator testCreateBPlusTreeBacked(char[] term) {
        SearchTermMetrics metrics = Reflection
                .newInstance(SearchTermMetrics.class, term.length, -1);
        return new BPlusTreeBackedTermIndexDeduplicator(term, metrics);
    }

    /**
     * Return an {@link LargeTermIndexDeduplicator} that is only appropriate for
     * unit tests.
     * 
     * @param term
     * @return the {@link LargeTermIndexDeduplicator}
     */
    static LargeTermIndexDeduplicator testCreateBruteForceBacked(char[] term) {
        SearchTermMetrics metrics = Reflection
                .newInstance(SearchTermMetrics.class, term.length, -1);
        return new BruteForceTermIndexDeduplicator(term, metrics);
    }

    /**
     * Return an {@link LargeTermIndexDeduplicator} that is only appropriate for
     * unit tests.
     * 
     * @param term
     * @return the {@link LargeTermIndexDeduplicator}
     */
    static LargeTermIndexDeduplicator testCreateChronicleBacked(char[] term) {
        SearchTermMetrics metrics = Reflection
                .newInstance(SearchTermMetrics.class, term.length, -1);
        return new ChronicleBackedTermIndexDeduplicator(term, metrics);
    }

    /**
     * If possible, return the amount of "direct" memory that is available for
     * off-heap storage.
     * 
     * @return the number of direct memory bytes that are available
     */
    @SuppressWarnings("restriction")
    private static long availableDirectMemory() {
        try {
            com.sun.management.OperatingSystemMXBean osBean = (com.sun.management.OperatingSystemMXBean) ManagementFactory
                    .getOperatingSystemMXBean();
            return osBean.getFreePhysicalMemorySize();
        }
        catch (Exception e) {}
        return 0;
    }

    /**
     * Formats a byte count into a human-readable string representation.
     * 
     * This method converts a raw byte count into a formatted string with the
     * most appropriate unit (B, KB, MB, GB, TB, PB, EB) based on the size. Once
     * the value exceeds 1,000 in one unit, it will be represented in the next
     * larger unit.
     * 
     * For example:
     * <ul>
     * <li>900 bytes will be formatted as "900 B"</li>
     * <li>1,500 bytes will be formatted as "1.5 KB"</li>
     * <li>1,500,000 bytes will be formatted as "1.5 MB"</li>
     * <li>343,597,383,520 bytes will be formatted as "343.6 GB"</li>
     * </ul>
     * 
     * The method uses decimal-based units (powers of 1,000) rather than
     * binary-based units (powers of 1,024).
     * 
     * @param bytes The number of bytes to format (must be positive)
     * @return A formatted string representing the bytes in the most appropriate
     *         unit
     * @throws IllegalArgumentException if the byte count is not positive
     */
    private static String formatBytes(long bytes) {
        Preconditions.checkArgument(bytes > 0);
        double unit = 1000;
        if(bytes < unit) {
            return bytes + " B";
        }

        String[] units = { "KB", "MB", "GB", "TB", "PB", "EB" };
        int unitIndex = (int) (Math.log10(bytes) / Math.log10(unit)) - 1;
        unitIndex = Math.min(unitIndex, units.length - 1);

        // Calculate the value in the determined unit
        double value = bytes / Math.pow(unit, unitIndex + 1);

        // Format with up to 2 decimal places, removing trailing zeros
        if(value == (long) value) {
            return String.format("%d %s", (long) value, units[unitIndex]);
        }
        else {
            return String.format("%.2f %s", value, units[unitIndex])
                    .replaceAll("\\.00$", "").replaceAll("(\\.[0-9])0$", "$1");
        }
    }

    /**
     * Retrieve the {@code text}'s end index.
     * 
     * @param text
     * @return the end index
     */
    private static int getTextEndPos(Text text) {
        return getTextInstanceVariableValue("end", text);
    }

    /**
     * Return the value of an instance {@code variable} from the provided
     * {@code text} or throw an exception if the {@code text} was not properly
     * creatted by {@link Text#wrap(char[], int, int) wrapping) a char[].
     * 
     * @param variable
     * @param text
     * @return the value for the variable
     */
    private static int getTextInstanceVariableValue(String variable,
            Text text) {
        try {
            int position = Reflection.get(variable, text);
            return position;
        }
        catch (Exception e) {
            throw new IllegalArgumentException(
                    "Can only use Text that is created for a char[]");
        }
    }

    /**
     * Retrieve the {@code text}'s start index.
     * 
     * @param text
     * @return the start index
     */
    private static int getTextStartPos(Text text) {
        return getTextInstanceVariableValue("start", text);
    }

    /**
     * Summarizes memory information into a well-formatted tabular string
     * representation with proper borders and alignment.
     * 
     * This method creates a formatted table showing three memory metrics:
     * <ul>
     * <li>Estimated Memory Required</li>
     * <li>Off-Heap Memory Available</li>
     * <li>Heap Memory Available</li>
     * </ul>
     * 
     * Each memory value is formatted in a human-readable way using appropriate
     * units (B, KB, MB, GB, TB, PB, EB) based on the size.
     * 
     * @param estimatedMemory The estimated memory required in bytes
     * @param offHeapMemory The available off-heap memory in bytes
     * @param heapMemory The available heap memory in bytes
     * @return A formatted string containing a nicely formatted table with
     *         memory information
     * @throws IllegalArgumentException if any memory value is negative
     */
    private static String summarizeMemory(long estimated, long direct,
            long heap) {
        Preconditions.checkArgument(estimated >= 0,
                "Estimated memory cannot be negative");
        Preconditions.checkArgument(direct >= 0,
                "Off heap memory cannot be negative");
        Preconditions.checkArgument(heap >= 0,
                "Heap memory cannot be negative");

        String formattedEstimated = formatBytes(estimated);
        String formattedOffHeap = formatBytes(direct);
        String formattedHeap = formatBytes(heap);

        // Define row descriptions with proper length
        String[] descriptions = { "Estimated Memory Required",
                "Off-Heap Memory Available", "Heap Memory Available" };
        String[] values = { formattedEstimated, formattedOffHeap,
                formattedHeap };

        // Find the longest description to determine column width
        int descColumnWidth = 0;
        for (String desc : descriptions) {
            descColumnWidth = Math.max(descColumnWidth, desc.length());
        }

        // Find the longest value to determine value column width
        int valueColumnWidth = 0;
        for (String value : values) {
            valueColumnWidth = Math.max(valueColumnWidth, value.length());
        }

        // Add padding
        descColumnWidth += 2;
        valueColumnWidth += 2;

        // Total table width
        int tableWidth = descColumnWidth + valueColumnWidth + 3; // +3 for
                                                                 // borders

        StringBuilder table = new StringBuilder();

        // Top border
        table.append("┌");
        for (int i = 0; i < descColumnWidth; i++) {
            table.append("─");
        }
        table.append("┬");
        for (int i = 0; i < valueColumnWidth; i++) {
            table.append("─");
        }
        table.append("┐\n");

        // Header
        String title = "Memory Status";
        int padding = (tableWidth - title.length() - 2) / 2;
        table.append("│");
        for (int i = 0; i < padding; i++) {
            table.append(" ");
        }
        table.append(title);
        for (int i = 0; i < tableWidth - padding - title.length() - 2; i++) {
            table.append(" ");
        }
        table.append("│\n");

        // Header separator
        table.append("├");
        for (int i = 0; i < descColumnWidth; i++) {
            table.append("─");
        }
        table.append("┬");
        for (int i = 0; i < valueColumnWidth; i++) {
            table.append("─");
        }
        table.append("┤\n");

        // Table content
        for (int i = 0; i < descriptions.length; i++) {
            String desc = descriptions[i];
            String value = values[i];

            // Ensure consistent spacing by adding exact number of spaces needed
            int descSpaces = descColumnWidth - desc.length() - 1;
            int valueSpaces = valueColumnWidth - value.length() - 1;

            table.append("│ ").append(desc);

            for (int j = 0; j < descSpaces; j++) {
                table.append(" ");
            }

            table.append("│ ").append(value);

            for (int j = 0; j < valueSpaces; j++) {
                table.append(" ");
            }

            table.append("│\n");

            // Add separator between rows (except after the last row)
            if(i < descriptions.length - 1) {
                table.append("├");
                for (int j = 0; j < descColumnWidth; j++) {
                    table.append("─");
                }
                table.append("┼");
                for (int j = 0; j < valueColumnWidth; j++) {
                    table.append("─");
                }
                table.append("┤\n");
            }
        }

        // Bottom border
        table.append("└");
        for (int i = 0; i < descColumnWidth; i++) {
            table.append("─");
        }
        table.append("┴");
        for (int i = 0; i < valueColumnWidth; i++) {
            table.append("─");
        }
        table.append("┘");

        return table.toString();
    }

    /**
     * The char array from the overall term that is being indexed. Required to
     * for serialization and deserialization to/from the container's underlying
     * store.
     */
    protected final char[] term;

    protected final SearchTermMetrics metrics;

    /**
     * Construct a new instance.
     * 
     * @param term
     * @param metrics
     */
    protected LargeTermIndexDeduplicator(char[] term,
            SearchTermMetrics metrics) {
        this.term = term;
        this.metrics = metrics;
    }

    @Override
    public boolean contains(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Text> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException();
    }

    /**
     * {@link LargeTermIndexDeduplicator} that is backed by a B+ Tree in a
     * {@link Path file}.
     *
     * @author Jeff Nelson
     */
    private static class BPlusTreeBackedTermIndexDeduplicator
            extends LargeTermIndexDeduplicator {

        /**
         * {@link Funnel} for {@link #filter}.
         */
        @SuppressWarnings("serial")
        private static Funnel<Text> FUNNEL = new Funnel<Text>() {

            @Override
            public void funnel(Text from, PrimitiveSink into) {
                into.putBytes(from.getBytes());
            }

        };

        /**
         * Known collisions that are stored in an effort to prevent a linear
         * probe of the underlying file. We only store known collisions here
         * (e.g. when a call to {@link #contains(byte[])} returns true) so this
         * does not have the same memory overhead as storing the entire
         * set of {@link Text}.
         */
        private final Set<Text> collisions = new HashSet<>();

        /**
         * A {@link BloomFilter} to speed up calls to {@link #contains(Object)}.
         */
        private final BloomFilter<Text> filter;

        /**
         * {@link Text} {@link Serializer} for {@link #tree}.
         */
        private final Serializer<Text> serializer = new Serializer<Text>() {

            @Override
            public int maxSize() {
                return 0;
            }

            @Override
            public Text read(LargeByteBuffer bb) {
                int start = bb.getInt();
                int end = bb.getInt();
                return Text.wrap(term, start, end);
            }

            @Override
            public void write(LargeByteBuffer bb, Text t) {
                int start = getTextStartPos(t);
                int end = getTextEndPos(t);
                bb.putInt(start);
                bb.putInt(end);
            }

        };

        /**
         * Disk based backing store to sort {@link Text} values by
         * {@link Text#hashCode() hash code)}.
         */
        private final BPlusTree<Integer, Text> tree;

        /**
         * Construct a new instance.
         */
        public BPlusTreeBackedTermIndexDeduplicator(char[] term,
                SearchTermMetrics metrics) {
            super(term, metrics);
            this.filter = BloomFilter.create(FUNNEL,
                    metrics.upperBoundOfPossibleSubstrings());
            this.tree = BPlusTree.file().directory(FileOps.tempDir(""))
                    .keySerializer(Serializer.INTEGER)
                    .valueSerializer(serializer).naturalOrder();
        }

        @Override
        public boolean add(Text text) {
            if(!contains(text)) {
                filter.put(text);
                tree.insert(text.hashCode(), text);
                return true;
            }
            else {
                return false;
            }
        }

        @Override
        public void close() throws IOException {
            try {
                tree.close();
            }
            catch (IOException e) {
                throw e;
            }
            catch (Exception e1) {
                throw CheckedExceptions.wrapAsRuntimeException(e1);
            }
        }

        @Override
        public boolean contains(Object o) {
            if(o instanceof Text) {
                Text text = (Text) o;
                if(filter.mightContain(text)) {
                    if(collisions.contains(text)) {
                        return true;
                    }
                    else {
                        Iterator<Text> it = tree.find(text.hashCode())
                                .iterator();
                        while (it.hasNext()) {
                            if(text.equals(it.next())) {
                                try {
                                    collisions.add(text);
                                }
                                catch (OutOfMemoryError e) {
                                    // Ignore and keep on trucking. We were
                                    // using this data structure because memory
                                    // was in short supply, but we kept an
                                    // in-memory of known collisions to try to
                                    // speed things up. If we're truly at a
                                    // point where memory isn't available, just
                                    // suffer the reality of needing a linear
                                    // probe to be aware of any new collisions.
                                }
                                return true;
                            }
                        }
                    }
                }
            }
            return false;
        }

        @Override
        public Iterator<Text> iterator() {
            return tree.findAll().iterator();
        }

        @Override
        public int size() {
            return Iterators.size(iterator());
        }
    }

    /**
     * A {@link LargeTermIndexDeduplicator} that uses a brute force approach to
     * check if an index from a term is considered a relative duplicate, without
     * using additional memory beyond that which is already utilized to store
     * the term.
     * <p>
     * <strong>NOTE:</strong> It is assumed that substrings are
     * {@link #add(Text) added} in depth first order.
     * </p>
     *
     * @author Jeff Nelson
     */
    private static class BruteForceTermIndexDeduplicator
            extends LargeTermIndexDeduplicator {

        /**
         * Returns true if the substring s[start..end) occurs at any earlier
         * position in s.
         *
         * @param s the input char array
         * @param start the inclusive start index of the substring
         * @param end the exclusive end index of the substring
         * @return true if an identical substring of the same length starts at
         *         some k < start
         */
        public static boolean substringAppearsEarlier(char[] s, int start,
                int end) {
            int len = end - start;
            if(len <= 0 || start <= 0) {
                return false;
            }
            // for each possible earlier start k where k + len ≤ s.length and k
            // < start
            int maxK = Math.min(start - len, s.length - len);
            for (int k = 0; k <= maxK; k++) {
                // compare s[k..k+len) to s[start..end)
                int i = 0;
                while (i < len && s[k + i] == s[start + i]) {
                    i++;
                }
                if(i == len) {
                    // full match found
                    return true;
                }
            }
            return false;
        }

        /**
         * Construct a new instance.
         * 
         * @param term
         * @param metrics
         */
        protected BruteForceTermIndexDeduplicator(char[] term,
                SearchTermMetrics metrics) {
            super(term, metrics);
        }

        @Override
        public boolean add(Text e) {
            int start = getTextStartPos(e);
            int end = getTextEndPos(e);
            return !substringAppearsEarlier(term, start, end);
        }

        @Override
        public void close() throws IOException {}

    }

    /**
     * {@link LargeTermIndexDeduplicator} that is backed by a
     * {@link ChronicleSet}.
     *
     * @author Jeff Nelson
     */
    private static class ChronicleBackedTermIndexDeduplicator
            extends LargeTermIndexDeduplicator {

        static {
            Logging.disable(Jvm.class);
            Logging.disable(InternalAnnouncer.class);
            Logging.disable(VanillaChronicleMap.class);
            Logging.disable(CompiledMapQueryContext.class);
        }

        /**
         * The backing store.
         */
        private final ChronicleSet<Text> chronicle;

        /**
         * Construct a new instance.
         * 
         * @param term
         * @param metrics
         */
        public ChronicleBackedTermIndexDeduplicator(char[] term,
                SearchTermMetrics metrics) {
            super(term, metrics);
            // @formatter:off
            this.chronicle = ChronicleSet.of(Text.class)
                    .averageKeySize(metrics.averageSubstringLength())
                    .keyMarshaller(TextBytesMarshaller.INSTANCE)
                    .entries(metrics.upperBoundOfPossibleSubstrings())
                    .create();
            // @formatter:on
        }

        @Override
        public boolean add(Text text) {
            return chronicle.add(text);
        }

        @Override
        public void close() throws IOException {
            chronicle.close();
        }

        @Override
        public boolean contains(Object o) {
            return chronicle.contains(o);
        }

        @Override
        public Iterator<Text> iterator() {
            return chronicle.iterator();
        }

        @Override
        public int size() {
            return chronicle.size();
        }

        /**
         * Used to serialize {@link Text} to/from a {@link ChronicleSet}.
         *
         * @author Jeff Nelson
         */
        private static final class TextBytesMarshaller implements
                BytesWriter<Text>,
                BytesReader<Text>,
                ReadResolvable<TextBytesMarshaller> {

            /**
             * Singleton.
             */
            static TextBytesMarshaller INSTANCE = new TextBytesMarshaller();

            @SuppressWarnings("rawtypes")
            @Override
            public Text read(Bytes in, Text using) {
                int size = in.readInt();
                ByteBuffer bytes = ByteBuffer.allocate(size);
                in.read(bytes);
                bytes.flip();
                return Text.fromByteBufferCached(bytes);
            }

            @Override
            public @NotNull TextBytesMarshaller readResolve() {
                return INSTANCE;
            }

            @SuppressWarnings("rawtypes")
            @Override
            public void write(Bytes out, Text toWrite) {
                byte[] bytes = ByteBuffers.getByteArray(toWrite.getBytes());
                out.writeInt(bytes.length);
                out.write(bytes);
            }

        }
    }

}
