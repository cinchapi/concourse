/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2014 Jeff Nelson, Cinchapi Software Collective
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
package org.cinchapi.concourse.server.storage.db;

import java.io.File;
import java.lang.reflect.Field;
import java.util.List;

import org.cinchapi.concourse.server.io.FileSystem;
import org.cinchapi.concourse.server.storage.Store;
import org.cinchapi.concourse.server.storage.StoreTest;
import org.cinchapi.concourse.server.storage.temp.Write;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Test;

/**
 * 
 * 
 * @author jnelson
 */
public class DatabaseTest extends StoreTest {

    private String current;

    @Test
    public void testDatabaseRemovesUnbalancedBlocksOnStartup() throws Exception {
        Database db = (Database) store;
        db.accept(Write.add(TestData.getString(), TestData.getTObject(),
                TestData.getLong()));
        db.triggerSync();
        db.stop();
        FileSystem.deleteDirectory(current + File.separator + "csb");
        FileSystem.mkdirs(current + File.separator + "csb");
        db = new Database(db.getBackingStore()); // simulate server restart
        db.start();
        Field cpb = db.getClass().getDeclaredField("cpb");
        Field csb = db.getClass().getDeclaredField("csb");
        Field ctb = db.getClass().getDeclaredField("ctb");
        cpb.setAccessible(true);
        csb.setAccessible(true);
        ctb.setAccessible(true);
        Assert.assertEquals(1, ((List<?>) ctb.get(db)).size());
        Assert.assertEquals(1, ((List<?>) csb.get(db)).size());
        Assert.assertEquals(1, ((List<?>) cpb.get(db)).size());
    }

    @Override
    protected void add(String key, TObject value, long record) {
        ((Database) store).accept(Write.add(key, value, record));
    }

    @Override
    protected void cleanup(Store store) {
        FileSystem.deleteDirectory(current);
    }

    @Override
    protected Database getStore() {
        current = TestData.DATA_DIR + File.separator + Time.now();
        return new Database(current);
    }

    @Override
    protected void remove(String key, TObject value, long record) {
        ((Database) store).accept(Write.remove(key, value, record));
    }

}
