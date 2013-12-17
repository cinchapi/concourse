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
import org.cinchapi.concourse.thrift.AccessToken;
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
public class AccessManagerTest extends ConcourseBaseTest {

    private String current = null;
    private AccessManager bouncer = null;

    @Rule
    public TestRule watcher = new TestWatcher() {

        @Override
        protected void finished(Description desc) {
            FileSystem.deleteFile(current);
        }

        @Override
        protected void starting(Description desc) {
            current = TestData.DATA_DIR + File.separator + Time.now();
            bouncer = AccessManager.create(current);

        }
    };

    @Test
    public void testDefaultAdminLogin() {
        ByteBuffer username = ByteBuffer.wrap("admin".getBytes());
        ByteBuffer password = ByteBuffer.wrap("admin".getBytes());
        Assert.assertTrue(bouncer
                .approve(username, password));
    }

    @Test
    public void testChangeAdminPassword() {
        ByteBuffer username = ByteBuffer.wrap("admin".getBytes());
        ByteBuffer password = ByteBuffer.wrap("admin".getBytes());
        ByteBuffer newPassword = ByteBuffer.wrap(TestData.getString()
                .getBytes());
        bouncer.grantAccess(username, newPassword);
        Assert.assertFalse(bouncer.approve(username,
                password));
        Assert.assertTrue(bouncer.approve(username,
                newPassword));
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
            Assert.assertTrue(bouncer.approve(
                    entry.getKey(), entry.getValue()));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCantRevokeAdmin() {
        bouncer.revokeAccess(toByteBuffer("admin"));
    }

    @Test
    public void testRevokeUser() {
        ByteBuffer username = toByteBuffer(TestData.getString());
        ByteBuffer password = toByteBuffer(TestData.getString());
        bouncer.grantAccess(username, password);
        bouncer.revokeAccess(username);
        Assert.assertFalse(bouncer.approve(username,
                password));
    }

    @Test
    public void testIsValidUsernameAndPassword() {
        ByteBuffer username = toByteBuffer(TestData.getString());
        ByteBuffer password = toByteBuffer(TestData.getString());
        ByteBuffer badpassword = toByteBuffer(TestData.getString() + "bad");
        bouncer.grantAccess(username, password);
        Assert.assertTrue(bouncer
                .approve(username, password));
        Assert.assertFalse(bouncer.approve(username,
                badpassword));
    }

    @Test
    public void testDiskSync() {
        Map<ByteBuffer, ByteBuffer> users = Maps.newHashMap();
        for (int i = 0; i < TestData.getScaleCount(); i++) {
            ByteBuffer username = toByteBuffer(TestData.getString());
            ByteBuffer password = toByteBuffer(TestData.getString());
            users.put(username, password);
            bouncer.grantAccess(username, password);
        }
        AccessManager bouncer2 = AccessManager.create(current);
        for (Entry<ByteBuffer, ByteBuffer> entry : users.entrySet()) {
            Assert.assertTrue(bouncer2.approve(
                    entry.getKey(), entry.getValue()));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCantCreateAccessTokenForInvalidUser() {
        bouncer.createAccessToken(toByteBuffer(TestData.getString() + "foo"));
    }

    @Test
    public void testCanCreateAccessTokenForValidUser() {
        ByteBuffer username = toByteBuffer(TestData.getString());
        ByteBuffer password = toByteBuffer(TestData.getString());
        bouncer.grantAccess(username, password);
        AccessToken token = bouncer.createAccessToken(username);
        Assert.assertTrue(bouncer.approve(token));
    }

    @Test
    public void testAccessTokenIsNotValidIfServerRestarts() {
        ByteBuffer username = toByteBuffer(TestData.getString());
        ByteBuffer password = toByteBuffer(TestData.getString());
        bouncer.grantAccess(username, password);
        AccessToken token = bouncer.createAccessToken(username);
        AccessManager bouncer2 = AccessManager.create(current); // simulate server
                                                    // restart by creating new
                                                    // bouncer
        Assert.assertFalse(bouncer2.approve(token));
    }
    
    @Test
    public void testAccessTokenIsNotValidIfPasswordChanges(){
        ByteBuffer username = toByteBuffer(TestData.getString());
        ByteBuffer password = toByteBuffer(TestData.getString());
        ByteBuffer password2 = toByteBuffer(TestData.getString());
        bouncer.grantAccess(username, password);
        AccessToken token = bouncer.createAccessToken(username);
        bouncer.grantAccess(username, password2);
        Assert.assertFalse(bouncer.approve(token));
    }
    
    @Test
    public void testAccessTokenIsNotValidIfAccessIsRevoked(){
        ByteBuffer username = toByteBuffer(TestData.getString());
        ByteBuffer password = toByteBuffer(TestData.getString());
        bouncer.grantAccess(username, password);
        AccessToken token = bouncer.createAccessToken(username);
        bouncer.revokeAccess(username);
        Assert.assertFalse(bouncer.approve(token));
    }

    /**
     * Convert a string to a ByteBuffer.
     * 
     * @param string
     * @return the bytebuffer
     */
    private static ByteBuffer toByteBuffer(String string) {
        return ByteBuffer.wrap(string.getBytes());
    }

}
