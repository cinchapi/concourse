/*
 * Copyright (c) 2013-2015 Cinchapi, Inc.
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
package org.cinchapi.concourse.server.storage.temp;

import java.util.Iterator;
import java.util.List;
import org.cinchapi.concourse.server.storage.StoreTest;
import org.cinchapi.concourse.server.storage.temp.Limbo;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Base unit tests for {@link Limbo} services.
 * 
 * @author Jeff Nelson
 */
public abstract class LimboTest extends StoreTest {

    @Test
    public void testIterator() {
        List<Write> writes = getWrites();
        for (Write write : writes) {
            add(write.getKey().toString(), write.getValue().getTObject(), write
                    .getRecord().longValue());
        }
        Iterator<Write> it0 = ((Limbo) store).iterator();
        Iterator<Write> it1 = writes.iterator();
        while (it1.hasNext()) {
            Assert.assertTrue(it0.hasNext());
            Write w0 = it0.next();
            Write w1 = it1.next();
            Assert.assertEquals(w0, w1);
        }
        Assert.assertFalse(it0.hasNext());
    }

    @Override
    protected abstract Limbo getStore();

    protected List<Write> getWrites() {
        List<Write> writes = Lists.newArrayList();
        for (int i = 0; i < (TestData.getScaleCount() * 50); i++) {
            writes.add(TestData.getWriteNotStorable());
        }
        return writes;
    }

    @Override
    protected void add(String key, TObject value, long record) {
        if (!store.verify(key, value, record)) {
            ((Limbo) store).insert(Write.add(key, value, record));
        }
    }

    @Override
    protected void remove(String key, TObject value, long record) {
        if (store.verify(key, value, record)) {
            ((Limbo) store).insert(Write.remove(key, value, record));
        }
    }

}
