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
package com.cinchapi.concourse.server.storage.db.kernel;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.io.Composite;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.model.Identifier;
import com.cinchapi.concourse.server.model.Position;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.server.storage.Action;
import com.cinchapi.concourse.server.storage.cache.BloomFilter;
import com.cinchapi.concourse.server.storage.db.CorpusRecord;
import com.cinchapi.concourse.server.storage.db.Revision;
import com.cinchapi.concourse.test.Variables;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.TStrings;
import com.cinchapi.concourse.util.TestData;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * Unit tests for
 * {@link com.cinchapi.concourse.server.storage.db.kernel.CorpusChunk}
 *
 * @author Jeff Nelson
 */
public class CorpusChunkTest extends ChunkTest<Text, Text, Position> {

    @Override
    protected Text getLocator() {
        return TestData.getText();
    }

    @Override
    protected Text getKey() {
        return TestData.getText();
    }

    @Override
    protected Position getValue() {
        return TestData.getPosition();
    }

    @Override
    protected Chunk<Text, Text, Position> create(BloomFilter filter) {
        return CorpusChunk.create(filter);
    }

    @Override
    protected Chunk<Text, Text, Position> load(Path file, BloomFilter filter,
            Manifest manifest) {
        return CorpusChunk.load(file, 0,
                FileSystem.getFileSize(file.toString()), filter, manifest);
    }

    @Override
    @Test(expected = IllegalStateException.class)
    public void testCannotInsertInImmutableChunk() {
        Text locator = Variables.register("locator", getLocator());
        Value value = Variables.register("value", getStringValue());
        Identifier record = Variables.register("record", getRecord());
        ((CorpusChunk) chunk).insert(locator, value, record, Time.now(),
                Action.ADD);
        if(chunk.length() <= 0) {
            value = Variables.register("value", getStringValue());
            ((CorpusChunk) chunk).insert(locator, value, record, Time.now(),
                    Action.ADD);
        }
        chunk.transfer(file);
        ((CorpusChunk) chunk).insert(locator, value, record, Time.now(),
                Action.ADD);
    }

    private Value getStringValue() {
        return Value.wrap(Convert.javaToThrift(TestData.getString()));
    }

    private Identifier getRecord() {
        return TestData.getIdentifier();
    }

    @Override
    @Test
    public void testMightContainLocatorKeyValue() {
        Value value = null;
        Text term = null;
        int position = 0;
        while (term == null) {
            value = Value.wrap(Convert.javaToThrift(TestData.getString()));
            position = 0;
            for (String string : value.getObject().toString().split(
                    TStrings.REGEX_GROUP_OF_ONE_OR_MORE_WHITESPACE_CHARS)) {
                if(!GlobalState.STOPWORDS.contains(string)
                        && !Strings.isNullOrEmpty(string)) {
                    term = Text.wrap(string);
                    break;
                }
                else {
                    position++;
                    continue;
                }
            }
        }
        doTestMightContainLocatorKeyValue(getLocator(), value, term,
                getRecord(), position);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMightContainLocatorKeyValueReproCON_1() {
        doTestMightContainLocatorKeyValue(
                Text.wrap("eqcicldw12dsowa7it4vi0pnqgewxci4c3ihyzf"),
                Value.wrap(Convert.javaToThrift(
                        "w jvnwa8xofm6asavrgpyxpk1mbgah7slcaookolqo fpa3g5 5csjly")),
                Text.wrap("w"), Identifier.of(52259011321627880L), 0);
    }

    @Test
    public void testMightContainLocatorKeyValueReproA() {
        Value value = null;
        Text term = null;
        int position = 0;
        while (term == null) {
            value = Value.wrap(Convert.javaToThrift("l  z15zses"));
            position = 0;
            for (String string : value.getObject().toString().split(
                    TStrings.REGEX_GROUP_OF_ONE_OR_MORE_WHITESPACE_CHARS)) {
                if(!GlobalState.STOPWORDS.contains(string)
                        && !Strings.isNullOrEmpty(string)) {
                    term = Text.wrap(string);
                    break;
                }
                else {
                    position++;
                    continue;
                }
            }
        }
        doTestMightContainLocatorKeyValue(getLocator(), value, term,
                getRecord(), position);
    }

    @Test
    public void testMightContainLocatorKeyValueReproB() {
        doTestMightContainLocatorKeyValue(Text.wrap(
                "7yubgpf x0 q1cj  52oiau do0034jq pj 02igw3w fbd5d2kw 5 1vwdbjoy4o6i4fgd"),
                Value.wrap(Convert.javaToThrift(
                        "mo48j2dgtkky48y5notzi8z6rhw6pio1rmmlptr0vcwfq8vzvwmvpawrfuo7d2t")),
                Text.wrap(
                        "mo48j2dgtkky48y5notzi8z6rhw6pio1rmmlptr0vcwfq8vzvwmvpawrfuo7d2t"),
                Identifier.of(5481930143744767354L), 0);
    }

    @Test
    public void testReproCON_4() {
        // TODO file this in jira
        Text key = Variables.register("key", Text.wrap("strings"));
        Identifier record = Variables.register("record", getRecord());
        Value value = Variables.register("value",
                Value.wrap(Convert.javaToThrift(
                        "aaihwopxetdxrumqlbjwgdsjgs tan rczlfjhyhlwhsr aqzpmquui mmmynpklmctgnonaaafagpjgv augolkz")));
        ((CorpusChunk) chunk).insert(key, value, record, Time.now(),
                Action.ADD);
        Text term = Variables.register("term", Text.wrap("aa"));
        Variables.register("chunkDump", chunk.dump());
        CorpusRecord searchRecord = CorpusRecord.createPartial(key, term);
        ((CorpusChunk) chunk).seek(Composite.create(key, term), searchRecord);
        Assert.assertTrue(
                searchRecord.get(term).stream().map(Position::getIdentifier)
                        .collect(Collectors.toCollection(LinkedHashSet::new))
                        .contains(record));
    }

    @Test
    public void testAsyncInsert() {// Verify that inserting data serially and
                                   // asynchronously produces a CorpusChunk with
                                   // the same content
        final Text aKey = Variables.register("aKey", TestData.getText());
        final Value aValue = Variables.register("aValue",
                Value.wrap(Convert.javaToThrift(TestData.getString()))); // force
                                                                         // string
        final Identifier aRecord = Variables.register("aRecord", getRecord());
        final long aTimestamp = Time.now();

        final Text bKey = Variables.register("bKey", TestData.getText());
        final Value bValue = Variables.register("bValue",
                Value.wrap(Convert.javaToThrift(TestData.getString()))); // force
                                                                         // string
        final Identifier bRecord = Variables.register("bRecord", getRecord());
        final long bTimestamp = Time.now();

        CorpusChunk serial = (CorpusChunk) create(BloomFilter.create(100));
        serial.insert(aKey, aValue, aRecord, aTimestamp, Action.ADD);
        serial.insert(bKey, bValue, bRecord, bTimestamp, Action.ADD);

        final CorpusChunk async = (CorpusChunk) chunk;

        Runnable a = new Runnable() {

            @Override
            public void run() {
                async.insert(aKey, aValue, aRecord, aTimestamp, Action.ADD);
            }

        };

        Runnable b = new Runnable() {

            @Override
            public void run() {
                async.insert(bKey, bValue, bRecord, bTimestamp, Action.ADD);
            }

        };

        ExecutorService executor = Executors.newFixedThreadPool(2);
        executor.execute(a);
        executor.execute(b);
        executor.shutdown();
        while (!executor.isTerminated()) {
            continue;
        }

        Assert.assertEquals(serial.dump().split("\n")[1],
                async.dump().split("\n")[1]); // must ignore the first line of
                                              // dump output which contains the
                                              // chunk id

    }

    @Test
    public void testDataDeduplication() {
        Text locator1 = Text.wrap("name");
        Text locator2 = Text.wrap("name");
        Value key1 = Value.wrap(Convert.javaToThrift("Fonamey"));
        Value key2 = Value.wrap(Convert.javaToThrift("Fonamey"));
        Identifier value1 = Identifier.of(1);
        Identifier value2 = Identifier.of(1);
        CorpusChunk corpus = (CorpusChunk) chunk;
        corpus.insert(locator2, key2, value2, Time.now(), Action.ADD);
        corpus.insert(locator1, key1, value1, Time.now(), Action.ADD);
        Position position = null;
        Iterator<Revision<Text, Text, Position>> it = corpus.iterator();
        while (it.hasNext()) {
            Revision<Text, Text, Position> revision = it.next();
            if(position == null) {
                position = revision.getValue();
            }
            Assert.assertSame(locator2, revision.getLocator());
            if(revision.getKey().toString().equals("name")) {
                Assert.assertSame(locator2, revision.getKey());
            }
            Assert.assertSame(position, revision.getValue());
        }
    }

    @Test
    public void testDoesNotAddDuplicates() {
        // LINE 20:
        // imwhrxxhtysldepivwwpbererstvplxnoknicpboajbdoayadaceldzbeasolxrnxcizcjjvymugsqyotcefeoohggsxaapnc
        // LINE 29:
        // rsikwrnyuvpxwufblpqxyhsmbphrepiickfmzivktvxoxfjrmwbmtbtkvczyptcgkpogdlnydqaatbsrhkyjrgjuyixyhtdngowj
        // Text key = Text.wrap("strings");
        // long i = 1;
        // Iterator<String> it = TestData.getWordsDotTxt().iterator();
        // while (i <= 29 && it.hasNext()) {
        // PrimaryKey record = PrimaryKey.wrap(i);
        // Value value = Value.wrap(Convert.javaToThrift(it.next()));
        // ((CorpusChunk) chunk).insert(key, value, record, Time.now(),
        // Action.ADD);
        // i++;
        // }
        // String[] lines = chunk.dump().split("\n");
        // Set<String> set = Sets.newHashSet();
        // for (String line : lines) {
        // set.add(line);
        // }
        // Assert.assertEquals(lines.length, set.size());
    }

    @Override
    @Test
    @Ignore
    public void testIterator() {
        // Direct insert for CorpusChunk is unsupported
    }

    /**
     * The implementation of {@link #testMightContainLocatorKeyValue()}.
     * 
     * @param locator
     * @param value
     * @param term
     * @param record
     * @param position
     */
    private void doTestMightContainLocatorKeyValue(Text locator, Value value,
            Text term, Identifier record, int position) {
        Preconditions.checkArgument(
                !GlobalState.STOPWORDS.contains(term.toString()));
        Variables.register("locator", locator);
        Variables.register("value", value);
        Variables.register("term", term);
        Variables.register("record", record);
        Variables.register("position", position);
        Assert.assertFalse(chunk.mightContain(Composite.create(locator, term,
                Position.of(record, position))));
        ((CorpusChunk) chunk).insert(locator, value, record, Time.now(),
                Action.ADD);
        Assert.assertTrue(chunk.mightContain(Composite.create(locator, term,
                Position.of(record, position))));
    }

}
