/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2015 Jeff Nelson, Cinchapi Software Collective
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.cinchapi.concourse.server.storage;

import java.io.File;
import java.lang.ref.WeakReference;

import org.cinchapi.concourse.ConcourseBaseTest;
import org.cinchapi.concourse.server.io.FileSystem;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests to ensure that {@link Transaction transactions} are properly
 * garbaged collected (e.g there are no memory leaks).
 * 
 * @author jnelson
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
        transaction.browse(1);
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
        a.browse(1);
        a.add("foo", TestData.getTObject(), 1);
        a.browse("foo");
        a.find("foo", Operator.GREATER_THAN, TestData.getTObject());
        a.commit();
        b.browse(1);
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
        transaction.browse(1);
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
