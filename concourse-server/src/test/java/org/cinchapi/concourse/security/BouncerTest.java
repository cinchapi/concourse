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
package org.cinchapi.concourse.security;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Map.Entry;

import org.cinchapi.concourse.ConcourseBaseTest;
import org.cinchapi.concourse.server.io.FileSystem;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.google.common.collect.Maps;

/**
 * 
 * 
 * @author jnelson
 */
public class BouncerTest extends ConcourseBaseTest {

    private String current = null;
    private Bouncer bouncer = null;

    @Rule
    public TestRule watcher = new TestWatcher() {

        @Override
        protected void finished(Description desc) {
            FileSystem.deleteFile(current);
        }

        @Override
        protected void starting(Description desc) {
            current = TestData.DATA_DIR + File.separator + Time.now();
            bouncer = Bouncer.create(current);

        }
    };

    @Test
    public void testDefaultAdminLogin() {
        ByteBuffer username = ByteBuffer.wrap("admin".getBytes());
        ByteBuffer password = ByteBuffer.wrap("admin".getBytes());
        Assert.assertTrue(bouncer.isValidUsernameAndPassword(username, password));
    }

    @Test
    public void testChangeAdminPassword() {
        ByteBuffer username = ByteBuffer.wrap("admin".getBytes());
        ByteBuffer password = ByteBuffer.wrap("admin".getBytes());
        ByteBuffer newPassword = ByteBuffer.wrap(TestData.getString()
                .getBytes());
        bouncer.grantAccess(username, newPassword);
        Assert.assertFalse(bouncer.isValidUsernameAndPassword(username, password));
        Assert.assertTrue(bouncer.isValidUsernameAndPassword(username, newPassword));
    }

    @Test
    public void testAddUsers() {
        Map<ByteBuffer, ByteBuffer> users = Maps.newHashMap();
        for (int i = 0; i < TestData.getScaleCount(); i++) {
            ByteBuffer username = toByteBuffer(TestData.getString());
            ByteBuffer password = toByteBuffer(TestData.getString());
            users.put(username, password);
            bouncer.grantAccess(username, password);
        }
        for (Entry<ByteBuffer, ByteBuffer> entry : users.entrySet()) {
            Assert.assertTrue(bouncer.isValidUsernameAndPassword(entry.getKey(),
                    entry.getValue()));
        }

    }

    private static ByteBuffer toByteBuffer(String string) {
        return ByteBuffer.wrap(string.getBytes());
    }

}
