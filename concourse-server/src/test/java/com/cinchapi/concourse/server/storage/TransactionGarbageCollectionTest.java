/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse.server.storage;

import java.io.File;
import java.lang.ref.WeakReference;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.storage.Engine;
import com.cinchapi.concourse.server.storage.Transaction;
import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.util.TestData;

/**
 * Unit tests to ensure that {@link Transaction transactions} are properly
 * garbaged collected (e.g there are no memory leaks).
 * 
 * @author Jeff Nelson
 */
public class TransactionGarbageCollectionTest extends ConcourseBaseTest {
    
    private Engine engine;
    private String directory;
    
    @Override
    public void beforeEachTest(){
        directory = TestData.getTemporaryTestDir();
        engine = new Engine(directory + File.separator + "buffer", directory
                + File.separator + "database");
        engine.start();
    }
    
    @Override
    public void afterEachTest(){
        FileSystem.deleteDirectory(directory);
    }
    
    @Test
    public void testGCAfterCommit(){
        Transaction transaction = engine.startTransaction();
        transaction.select(1);
        transaction.add("foo", TestData.getTObject(), 1);
        transaction.browse("foo");
        transaction.find("foo", Operator.GREATER_THAN, TestData.getTObject());
        transaction.commit();
        WeakReference<Transaction> reference = new WeakReference<Transaction>(transaction);
        Assert.assertNotNull(reference.get());
        transaction = null;
        System.gc();
        Assert.assertNull(reference.get()); 
    }
    
    @Test
    public void testGCAfterFailure(){
        Transaction a = engine.startTransaction();
        Transaction b = engine.startTransaction();
        a.select(1);
        a.add("foo", TestData.getTObject(), 1);
        a.browse("foo");
        a.find("foo", Operator.GREATER_THAN, TestData.getTObject());
        a.commit();
        b.select(1);
        b.add("foo", TestData.getTObject(), 1);
        b.browse("foo");
        b.find("foo", Operator.GREATER_THAN, TestData.getTObject());  
        WeakReference<Transaction> aa = new WeakReference<Transaction>(a);
        WeakReference<Transaction> bb = new WeakReference<Transaction>(b);
        Assert.assertNotNull(aa.get());
        Assert.assertNotNull(bb.get());
        b.commit();
        a = null;
        b = null;
        System.gc();
        Assert.assertNull(aa.get());
        Assert.assertNull(bb.get());
    }
    
    @Test
    public void testGCAfterAbort(){
        Transaction transaction = engine.startTransaction();
        transaction.select(1);
        transaction.add("foo", TestData.getTObject(), 1);
        transaction.browse("foo");
        transaction.find("foo", Operator.GREATER_THAN, TestData.getTObject());
        WeakReference<Transaction> reference = new WeakReference<Transaction>(transaction);
        Assert.assertNotNull(reference.get());
        transaction.abort();
        transaction = null;
        System.gc();
        Assert.assertNull(reference.get());        
    }
    
    @Test
    public void testGCAfterRangeLockUpgradeAndCommit(){
        Transaction transaction = engine.startTransaction();
        TObject value = TestData.getTObject();
        transaction.find("foo", Operator.EQUALS, value);
        transaction.add("foo", value, 1);
        WeakReference<Transaction> reference = new WeakReference<Transaction>(transaction);
        Assert.assertNotNull(reference.get());
        transaction.commit();
        transaction = null;
        System.gc();
        Assert.assertNull(reference.get()); 
    }

}
