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
package com.cinchapi.concourse.server.storage.db;

// import java.util.Iterator;
// import java.util.Set;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.model.Position;
import com.cinchapi.concourse.server.model.PrimaryKey;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.server.storage.Action;
import com.cinchapi.concourse.server.storage.db.Block;
import com.cinchapi.concourse.server.storage.db.Record;
import com.cinchapi.concourse.server.storage.db.SearchBlock;
import com.cinchapi.concourse.server.storage.db.SearchRecord;
import com.cinchapi.concourse.test.Variables;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.TStrings;
import com.cinchapi.concourse.util.TestData;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

// import com.google.common.collect.Sets;

/**
 * 
 * 
 * @author Jeff Nelson
 */
public class SearchBlockTest extends BlockTest<Text, Text, Position> {

    @Override
    @Test(expected = IllegalStateException.class)
    public void testCannotInsertInImmutableBlock() {
        Text locator = Variables.register("locator", getLocator());
        Value value = Variables.register("value", getStringValue());
        PrimaryKey record = Variables.register("record", getRecord());
        ((SearchBlock) block).insert(locator, value, record, Time.now(),
                Action.ADD);
        if(block.size() <= 0) {
            value = Variables.register("value", getStringValue());
            ((SearchBlock) block).insert(locator, value, record, Time.now(),
                    Action.ADD);
        }
        block.sync();
        ((SearchBlock) block).insert(locator, value, record, Time.now(),
                Action.ADD);
    }

    private Value getStringValue() {
        return Value.wrap(Convert.javaToThrift(TestData.getString()));
    }

    private PrimaryKey getRecord() {
        return TestData.getPrimaryKey();
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
            for (String string : value
                    .getObject()
                    .toString()
                    .split(TStrings.REGEX_GROUP_OF_ONE_OR_MORE_WHITESPACE_CHARS)) {
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
                Value.wrap(Convert
                        .javaToThrift("w jvnwa8xofm6asavrgpyxpk1mbgah7slcaookolqo fpa3g5 5csjly")),
                Text.wrap("w"), PrimaryKey.wrap(52259011321627880L), 0);
    }

    @Test
    public void testMightContainLocatorKeyValueReproA() {
        Value value = null;
        Text term = null;
        int position = 0;
        while (term == null) {
            value = Value.wrap(Convert.javaToThrift("l  z15zses"));
            position = 0;
            for (String string : value
                    .getObject()
                    .toString()
                    .split(TStrings.REGEX_GROUP_OF_ONE_OR_MORE_WHITESPACE_CHARS)) {
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
    public void testReproCON_4() {
        // TODO file this in jira
        Text key = Variables.register("key", Text.wrap("strings"));
        PrimaryKey record = Variables.register("record", getRecord());
        Value value = Variables
                .register(
                        "value",
                        Value.wrap(Convert
                                .javaToThrift("aaihwopxetdxrumqlbjwgdsjgs tan rczlfjhyhlwhsr aqzpmquui mmmynpklmctgnonaaafagpjgv augolkz")));
        ((SearchBlock) block)
                .insert(key, value, record, Time.now(), Action.ADD);
        Text term = Variables.register("term", Text.wrap("aa"));
        Variables.register("blockDump", block.dump());
        SearchRecord searchRecord = Record.createSearchRecordPartial(key, term);
        ((SearchBlock) block).seek(key, term, searchRecord);
        Assert.assertTrue(searchRecord.search(term).contains(record));
    }

    @Test
    public void testAsyncInsert() {// Verify that inserting data serially and
                                   // asynchronously produces a SearchBlock with
                                   // the same content
        final Text aKey = Variables.register("aKey", TestData.getText());
        final Value aValue = Variables.register("aValue",
                Value.wrap(Convert.javaToThrift(TestData.getString()))); // force
                                                                         // string
        final PrimaryKey aRecord = Variables.register("aRecord", getRecord());
        final long aTimestamp = Time.now();

        final Text bKey = Variables.register("bKey", TestData.getText());
        final Value bValue = Variables.register("bValue",
                Value.wrap(Convert.javaToThrift(TestData.getString()))); // force
                                                                         // string
        final PrimaryKey bRecord = Variables.register("bRecord", getRecord());
        final long bTimestamp = Time.now();

        SearchBlock serial = getMutableBlock(directory);
        serial.insert(aKey, aValue, aRecord, aTimestamp, Action.ADD);
        serial.insert(bKey, bValue, bRecord, bTimestamp, Action.ADD);

        final SearchBlock async = (SearchBlock) block;

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
                                              // block id

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
        // ((SearchBlock) block).insert(key, value, record, Time.now(),
        // Action.ADD);
        // i++;
        // }
        // String[] lines = block.dump().split("\n");
        // Set<String> set = Sets.newHashSet();
        // for (String line : lines) {
        // set.add(line);
        // }
        // Assert.assertEquals(lines.length, set.size());
    }

    @Override
    @Test
    @Ignore
    public void testCannotGetIteratorForMutableBlock() {
        // Direct insert for SearchBlock is unsupported
    }
    
    @Override
    @Test
    @Ignore
    public void testIterator(){
     // Direct insert for SearchBlock is unsupported
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
            Text term, PrimaryKey record, int position) {
        Preconditions.checkArgument(!GlobalState.STOPWORDS.contains(term
                .toString()));
        Variables.register("locator", locator);
        Variables.register("value", value);
        Variables.register("term", term);
        Variables.register("record", record);
        Variables.register("position", position);
        Assert.assertFalse(block.mightContain(locator, term,
                Position.wrap(record, position)));
        ((SearchBlock) block).insert(locator, value, record, Time.now(),
                Action.ADD);
        Assert.assertTrue(block.mightContain(locator, term,
                Position.wrap(record, position)));
    }

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
    protected SearchBlock getMutableBlock(String directory) {
        return Block.createSearchBlock(Long.toString(Time.now()), directory);
    }

}
