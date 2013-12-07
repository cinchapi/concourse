/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013 Jeff Nelson, Cinchapi Software Collective
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
package org.cinchapi.concourse.server.storage.temp;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import org.cinchapi.concourse.server.io.FileSystem;
import org.cinchapi.concourse.server.storage.PermanentStore;
import org.cinchapi.concourse.server.storage.Store;
import org.cinchapi.concourse.server.storage.temp.Buffer;
import org.cinchapi.concourse.server.storage.temp.Limbo;
import org.cinchapi.concourse.testing.Variables;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link Buffer}.
 * 
 * @author jnelson
 */
public class BufferTest extends LimboTest {

    private static PermanentStore MOCK_DESTINATION = Mockito
            .mock(PermanentStore.class);
    static {
        // NOTE: The Buffer assumes it is transporting to a Database, but we
        // cannot mock that class with Mockito since it is final. Mocking the
        // PermanentStore interface does not pose a problem as long as tests
        // don't do something that would cause the Database#triggerSync() method
        // to be called (i.e. transporting more than a page worth of Writes).
        //
        // So, please use the Buffer#canTransport() method to check to see if is
        // okay to do a transport without causing a triggerSync(). And do not
        // unit tests streaming writes in this test class (do that at a level
        // above where an actual Database is defined)!!!
        Mockito.doNothing().when(MOCK_DESTINATION)
                .accept(Mockito.any(Write.class));
    }

    private String current;

    @Override
    protected Buffer getStore() {
        current = TestData.DATA_DIR + File.separator + Time.now();
        return new Buffer(current);
    }

    @Override
    protected void cleanup(Store store) {
        FileSystem.deleteDirectory(current);
    }

    @Test
    public void testIteratorAfterTransport() {
        List<Write> writes = getWrites();
        int j = 0;
        for (Write write : writes) {
            add(write.getKey().toString(), write.getValue().getTObject(), write
                    .getRecord().longValue());
            Variables.register("write_" + j, write);
            j++;
        }
        Variables.register("size_pre_transport", writes.size());
        int div = Variables.register("div", (TestData.getScaleCount() % 9) + 1);
        int count = Variables.register("count", writes.size() / div);
        for (int i = 0; i < count; i++) {
            if(((Buffer) store).canTransport()) {
                ((Buffer) store).transport(MOCK_DESTINATION);
                writes.remove(0);
            }
            else {
                break;
            }
        }
        Variables.register("size_post_transport", writes.size());
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

    @Test
    public void testReverseIteratorAfterTransport() {
        List<Write> writes = getWrites();
        for (Write write : writes) {
            add(write.getKey().toString(), write.getValue().getTObject(), write
                    .getRecord().longValue());
        }
        int div = (TestData.getScaleCount() % 9) + 1;
        int count = writes.size() / div;
        for (int i = 0; i < count; i++) {
            if(((Buffer) store).canTransport()) {
                ((Buffer) store).transport(MOCK_DESTINATION);
                writes.remove(0);
            }
            else {
                break;
            }
        }
        Iterator<Write> it = ((Limbo) store).reverseIterator();
        int index = writes.size() - 1;
        while (it.hasNext()) {
            if(index == 0) {}
            Write w = it.next();
            Assert.assertEquals(w, writes.get(index));
            index--;
        }
        Assert.assertEquals(-1, index);
    }

}
