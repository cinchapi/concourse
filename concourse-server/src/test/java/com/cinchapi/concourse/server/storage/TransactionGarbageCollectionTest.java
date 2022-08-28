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
import java.util.Map;
import java.util.Map.Entry;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentMap;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.server.concurrent.Token;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.util.TestData;
import com.google.common.cache.Cache;
import com.google.common.collect.RangeSet;

/**
 * Unit tests to ensure that
 * {@link com.cinchapi.concourse.server.storage.Transaction transactions}
 * are properly garbaged collected (e.g there are no memory leaks).
 *
 * @author Jeff Nelson
 */
public class TransactionGarbageCollectionTest extends ConcourseBaseTest {

    /**
     * Return {@code true} if {@code engine} has a {@link VersionChangeListener}
     * with a {@link Object#toString() toString} value matching {@code label}.
     * 
     * @param engine
     * @param label
     * @return {@code true} if ant aptly labeled {@link VersionChangeListener}
     *         is contained in the {@code engine}
     */
    private static boolean containsVersionChangeListenerWithLabel(Engine engine,
            String label) {
        for (VersionChangeListener listener : getRangeVersionChangeListeners(
                engine).asMap().keySet()) {
            if(listener.toString().equals(label)) {
                return true;
            }
        }
        for (Entry<Token, WeakHashMap<VersionChangeListener, Boolean>> entry : getVersionChangeListeners(
                engine).entrySet()) {
            for (VersionChangeListener listener : entry.getValue().keySet()) {
                if(listener.toString().equals(label)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Return the collection of range {@link VersionChangeListener
     * VersionChangeListeners} from {@code engine}.
     * 
     * @param engine
     * @return the range {@link VersionChangeListener VersionChangeListeners}
     */
    private static Cache<VersionChangeListener, Map<Text, RangeSet<Value>>> getRangeVersionChangeListeners(
            Engine engine) {
        return Reflection.get("rangeVersionChangeListeners", engine);
    }

    /**
     * Return the collection of non-range {@link VersionChangeListener
     * VersionChangeListeners} from {@code engine}.
     * 
     * @param engine
     * @return the non-range {@link VersionChangeListener
     *         VersionChangeListeners}
     */
    private static ConcurrentMap<Token, WeakHashMap<VersionChangeListener, Boolean>> getVersionChangeListeners(
            Engine engine) {
        return Reflection.get("versionChangeListeners", engine);
    }

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
        String label = transaction.toString();
        Assert.assertTrue(
                containsVersionChangeListenerWithLabel(engine, label));
        transaction.abort();
        transaction = null;
        System.gc();
        Assert.assertFalse(
                containsVersionChangeListenerWithLabel(engine, label));
    }

    @Test
    public void testGCAfterCommit() {
        Transaction transaction = engine.startTransaction();
        transaction.select(1);
        transaction.add("foo", TestData.getTObject(), 1);
        transaction.browse("foo");
        transaction.find("foo", Operator.GREATER_THAN, TestData.getTObject());
        transaction.commit();
        String label = transaction.toString();
        Assert.assertTrue(
                containsVersionChangeListenerWithLabel(engine, label));
        transaction = null;
        System.gc();
        Assert.assertFalse(
                containsVersionChangeListenerWithLabel(engine, label));
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

        String aLabel = a.toString();
        String bLabel = b.toString();
        Assert.assertTrue(
                containsVersionChangeListenerWithLabel(engine, aLabel));
        Assert.assertTrue(
                containsVersionChangeListenerWithLabel(engine, bLabel));
        a.commit();
        a = null;
        b = null;
        System.gc();
        Assert.assertFalse(
                containsVersionChangeListenerWithLabel(engine, aLabel));
        Assert.assertFalse(
                containsVersionChangeListenerWithLabel(engine, bLabel));
    }

    @Test
    public void testGCAfterRangeLockUpgradeAndCommit() {
        Transaction transaction = engine.startTransaction();
        TObject value = TestData.getTObject();
        transaction.find("foo", Operator.EQUALS, value);
        transaction.add("foo", value, 1);
        String label = transaction.toString();
        Assert.assertTrue(
                containsVersionChangeListenerWithLabel(engine, label));
        transaction.commit();
        transaction = null;
        System.gc();
        Assert.assertFalse(
                containsVersionChangeListenerWithLabel(engine, label));;
    }

}
