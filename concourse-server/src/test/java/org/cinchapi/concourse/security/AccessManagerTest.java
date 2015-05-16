/*
 * Copyright (c) 2013-2015 Cinchapi, Inc.
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
package org.cinchapi.concourse.security;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.cinchapi.concourse.ConcourseBaseTest;
import org.cinchapi.concourse.server.io.FileSystem;
import org.cinchapi.concourse.thrift.AccessToken;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.ByteBuffers;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Unit tests for {@link AccessManager}.
 * 
 * @author Jeff Nelson
 */
public class AccessManagerTest extends ConcourseBaseTest {

    private String current = null;
    private AccessManager manager = null;

    @Rule
    public TestRule watcher = new TestWatcher() {

        @Override
        protected void finished(Description desc) {
            FileSystem.deleteFile(current);
        }

        @Override
        protected void starting(Description desc) {
            current = TestData.DATA_DIR + File.separator + Time.now();
            manager = AccessManager.create(current);

        }
    };

    @Test
    public void testDefaultAdminLogin() {
        ByteBuffer username = ByteBuffer.wrap("admin".getBytes());
        ByteBuffer password = ByteBuffer.wrap("admin".getBytes());
        Assert.assertNotNull(manager.authenticate(username, password));
    }

    @Test
    public void testChangeAdminPassword() {
        ByteBuffer username = ByteBuffer.wrap("admin".getBytes());
        ByteBuffer password = ByteBuffer.wrap("admin".getBytes());
        ByteBuffer newPassword = getSecurePassword();
        manager.createUser(username, newPassword);
        Assert.assertNull(manager.authenticate(username, password));
        Assert.assertNotNull(manager.authenticate(username, newPassword));
    }

    @Test
    public void testAddUsers() {
        Map<ByteBuffer, ByteBuffer> users = Maps.newHashMap();
        for (int i = 0; i < TestData.getScaleCount(); i++) {
            ByteBuffer username = getAcceptableUsername();
            ByteBuffer password = getSecurePassword();
            users.put(username, password);
            manager.createUser(username, password);
        }
        for (Entry<ByteBuffer, ByteBuffer> entry : users.entrySet()) {
            Assert.assertNotNull(manager.authenticate(entry.getKey(),
                    entry.getValue()));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCantDeleteAdmin() {
        manager.deleteUser(toByteBuffer("admin"));
    }

    @Test
    public void testDeleteUser() {
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        manager.createUser(username, password);
        manager.deleteUser(username);
        Assert.assertFalse(manager.isExistingUsername(username));
        Assert.assertNull(manager.authenticate(username, password));
    }

    @Test
    public void testIsValidUsernameAndPassword() {
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        ByteBuffer badpassword = toByteBuffer(TestData.getString() + "bad");
        manager.createUser(username, password);
        Assert.assertNotNull(manager.authenticate(username, password));
        Assert.assertNull(manager.authenticate(username, badpassword));
    }

    @Test
    public void testDiskSync() {
        Map<ByteBuffer, ByteBuffer> users = Maps.newHashMap();
        for (int i = 0; i < TestData.getScaleCount(); i++) {
            ByteBuffer username = getAcceptableUsername();
            ByteBuffer password = getSecurePassword();
            users.put(username, password);
            manager.createUser(username, password);
        }
        AccessManager manager2 = AccessManager.create(current);
        for (Entry<ByteBuffer, ByteBuffer> entry : users.entrySet()) {
            Assert.assertNotNull(manager2.authenticate(entry.getKey(),
                    entry.getValue()));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCantCreateAccessTokenForInvalidUser() {
        manager.getNewAccessToken(toByteBuffer(TestData.getString() + "foo"));
    }

    @Test
    public void testCanCreateAccessTokenForValidUser() {
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        manager.createUser(username, password);
        AccessToken token = manager.getNewAccessToken(username);
        Assert.assertTrue(manager.authorize(token));
    }

    @Test
    public void testAccessTokenIsNotValidIfServerRestarts() {
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        manager.createUser(username, password);
        AccessToken token = manager.getNewAccessToken(username);
        AccessManager manager2 = AccessManager.create(current); // simulate
                                                                // server
                                                                // restart by
                                                                // creating new
                                                                // manager
        Assert.assertFalse(manager2.authorize(token));
    }

    @Test
    public void testAccessTokenIsNotValidIfPasswordChanges() {
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        ByteBuffer password2 = getSecurePassword();
        manager.createUser(username, password);
        AccessToken token = manager.getNewAccessToken(username);
        manager.createUser(username, password2);
        Assert.assertFalse(manager.authorize(token));
    }

    @Test
    public void testAccessTokenIsNotValidIfAccessIsRevoked() {
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        manager.createUser(username, password);
        AccessToken token = manager.getNewAccessToken(username);
        manager.deleteUser(username);
        Assert.assertFalse(manager.authorize(token));
    }

    @Test
    public void testAccessTokenAutoExpiration() throws InterruptedException {
        manager = AccessManager.createForTesting(current, 60,
                TimeUnit.MILLISECONDS);
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        manager.createUser(username, password);
        AccessToken token = manager.getNewAccessToken(username);
        TimeUnit.MILLISECONDS.sleep(60);
        Assert.assertFalse(manager.authorize(token));
    }

    @Test
    public void testInvalidateAccessToken() {
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        manager.createUser(username, password);
        AccessToken token = manager.getNewAccessToken(username);
        manager.deauthorize(token);
        Assert.assertFalse(manager.authorize(token));
    }

    @Test
    public void testTwoAccessTokensForSameUser() {
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        manager.createUser(username, password);
        AccessToken token1 = manager.getNewAccessToken(username);
        AccessToken token2 = manager.getNewAccessToken(username);
        Assert.assertNotEquals(token1, token2);
        Assert.assertTrue(manager.authorize(token1));
        Assert.assertTrue(manager.authorize(token2));
    }

    @Test
    public void testInvalidatingOneAccessTokenDoesNotAffectOther() {
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        manager.createUser(username, password);
        AccessToken token1 = manager.getNewAccessToken(username);
        AccessToken token2 = manager.getNewAccessToken(username);
        manager.deauthorize(token2);
        Assert.assertTrue(manager.authorize(token1));
    }

    @Test
    public void testRevokingAccessInvalidatesAllAccessTokens() {
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        manager.createUser(username, password);
        List<AccessToken> tokens = Lists.newArrayList();
        for (int i = 0; i < TestData.getScaleCount(); i++) {
            tokens.add(manager.getNewAccessToken(username));
        }
        manager.deleteUser(username);
        for (AccessToken token : tokens) {
            Assert.assertFalse(manager.authorize(token));
        }
    }

    @Test
    public void testChangingPasswordInvalidatesAllAccessTokens() {
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        manager.createUser(username, password);
        List<AccessToken> tokens = Lists.newArrayList();
        for (int i = 0; i < TestData.getScaleCount(); i++) {
            tokens.add(manager.getNewAccessToken(username));
        }
        manager.createUser(username, getSecurePassword());
        for (AccessToken token : tokens) {
            Assert.assertFalse(manager.authorize(token));
        }
    }

    @Test
    public void testEmptyPasswordNotSecure() {
        Assert.assertFalse(AccessManager.isSecurePassword(ByteBuffers
                .fromString("")));
    }

    @Test
    public void testAllWhitespacePasswordNotSecure() {
        Assert.assertFalse(AccessManager.isSecurePassword(ByteBuffers
                .fromString("     ")));
    }

    @Test
    public void testUsernameWithWhitespaceNotAcceptable() {
        Assert.assertFalse(AccessManager.isAcceptableUsername(ByteBuffers
                .fromString("   f  ")));
    }

    /**
     * Convert a string to a ByteBuffer.
     * 
     * @param string
     * @return the bytebuffer
     */
    protected static ByteBuffer toByteBuffer(String string) {
        return ByteBuffer.wrap(string.getBytes());
    }

    /**
     * Return a username that will pass the acceptance test.
     * 
     * @return username
     */
    protected static ByteBuffer getAcceptableUsername() {
        ByteBuffer username = null;
        while (username == null
                || !AccessManager.isAcceptableUsername(username)) {
            username = toByteBuffer(TestData.getString());
        }
        return username;
    }

    /**
     * Return a password that will pass the security test.
     * 
     * @return password
     */
    protected static ByteBuffer getSecurePassword() {
        ByteBuffer password = null;
        while (password == null || !AccessManager.isSecurePassword(password)) {
            password = toByteBuffer(TestData.getString());
        }
        return password;
    }

}
