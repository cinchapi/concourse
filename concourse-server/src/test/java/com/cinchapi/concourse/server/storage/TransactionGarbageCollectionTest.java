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
package com.cinchapi.concourse.server.storage;

import java.io.File;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.util.TestData;

/**
 * Unit tests to ensure that
 * {@link com.cinchapi.concourse.server.storage.Transaction transactions}
 * are properly garbaged collected (e.g there are no memory leaks).
 *
 * @author Jeff Nelson
 */
public class TransactionGarbageCollectionTest extends ConcourseBaseTest {

    private Engine engine;

    private String directory;

    @Override
    public void afterEachTest() {
        FileSystem.deleteDirectory(directory);
    }

    @Override
    public void beforeEachTest() {
        directory = TestData.getTemporaryTestDir();
        engine = new Engine(directory + File.separator + "buffer",
                directory + File.separator + "database");
        engine.start();
    }

    @Test
    public void testGCAfterAbort() {
        Transaction transaction = engine.startTransaction();
        transaction.select(1);
        transaction.add("foo", TestData.getTObject(), 1);
        transaction.browse("foo");
        transaction.find("foo", Operator.GREATER_THAN, TestData.getTObject());
        Assert.assertTrue(engine.containsTokenEventObserver(transaction));
        transaction.abort();
        Assert.assertFalse(engine.containsTokenEventObserver(transaction));
    }

    @Test
    public void testGCAfterCommit() {
        Transaction transaction = engine.startTransaction();
        transaction.select(1);
        transaction.add("foo", TestData.getTObject(), 1);
        transaction.browse("foo");
        transaction.find("foo", Operator.GREATER_THAN, TestData.getTObject());
        transaction.commit();
        Assert.assertFalse(engine.containsTokenEventObserver(transaction));
    }

    @Test
    public void testGCAfterFailure() {
        Transaction a = engine.startTransaction();
        Transaction b = engine.startTransaction();

        a.select(1);
        b.select(1);

        b.add("foo", TestData.getTObject(), 1);
        a.add("foo", TestData.getTObject(), 1);

        a.browse("foo");
        b.browse("foo");

        b.find("foo", Operator.GREATER_THAN, TestData.getTObject());
        a.find("foo", Operator.GREATER_THAN, TestData.getTObject());

        Assert.assertTrue(engine.containsTokenEventObserver(a));
        Assert.assertTrue(engine.containsTokenEventObserver(b));

        a.commit();

        Assert.assertFalse(engine.containsTokenEventObserver(a));
        Assert.assertFalse(engine.containsTokenEventObserver(b));
    }

    @Test
    public void testGCAfterRangeLockUpgradeAndCommit() {
        Transaction transaction = engine.startTransaction();
        TObject value = TestData.getTObject();
        transaction.find("foo", Operator.EQUALS, value);
        transaction.add("foo", value, 1);
        Assert.assertTrue(engine.containsTokenEventObserver(transaction));
        transaction.commit();
        Assert.assertFalse(engine.containsTokenEventObserver(transaction));
    }

}
