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
package org.cinchapi.concourse.security;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Unit tests for {@link AccessManager}.
 * 
 * @author jnelson
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
        Assert.assertTrue(manager.isValidUserNamePasswordCombo(username,
                password));
    }

    @Test
    public void testChangeAdminPassword() {
        ByteBuffer username = ByteBuffer.wrap("admin".getBytes());
        ByteBuffer password = ByteBuffer.wrap("admin".getBytes());
        ByteBuffer newPassword = getSecurePassword();
        manager.createUser(username, newPassword);
        Assert.assertFalse(manager.isValidUserNamePasswordCombo(username,
                password));
        Assert.assertTrue(manager.isValidUserNamePasswordCombo(username,
                newPassword));
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
            Assert.assertTrue(manager.isValidUserNamePasswordCombo(
                    entry.getKey(), entry.getValue()));
        }
    }

    @Test
    public void testAllUsersHaveUniqueUids() {
        Set<ByteBuffer> emptySet = Sets.newHashSet();
        Set<ByteBuffer> users = (Set<ByteBuffer>) addMoreUsers(emptySet,
                manager);
        Set<Short> uniqueUids = Sets.newHashSet();
        for (ByteBuffer username : users) {
            short uid = manager.getUidByUsername(username);
            Assert.assertFalse(uniqueUids.contains(uid)); // check uniqueness
            uniqueUids.add(uid);
        }
        AccessManager manager2 = AccessManager.create(current); // simulate
                                                                // server
                                                                // restart by
                                                                // creating new
                                                                // manager
        users = (Set<ByteBuffer>) addMoreUsers(users, manager2);
        uniqueUids = Sets.newHashSet();
        for (ByteBuffer username : users) {
            short uid = manager2.getUidByUsername(username);
            Assert.assertFalse(uniqueUids.contains(uid)); // check uniqueness
            uniqueUids.add(uid);
        }
    }

    @Test
    public void testAllUsersHaveUniqueUidsAfterSomeUserDeletions() {
        List<ByteBuffer> emptyList = Lists.newArrayList();
        List<ByteBuffer> users = (List<ByteBuffer>) addMoreUsers(emptyList,
                manager);
        users = deleteSomeUsers(users, manager);
        Set<Short> uniqueUids = Sets.newHashSet();
        for (ByteBuffer username : users) {
            short uid = manager.getUidByUsername(username);
            Assert.assertFalse(uniqueUids.contains(uid)); // check uniqueness
            uniqueUids.add(uid);
        }
        AccessManager manager2 = AccessManager.create(current); // simulate
                                                                // server
                                                                // restart by
                                                                // creating new
                                                                // manager
        users = deleteSomeUsers(users, manager2);
        users = (List<ByteBuffer>) addMoreUsers(users, manager2);
        uniqueUids = Sets.newHashSet();
        for (ByteBuffer username : users) {
            short uid = manager2.getUidByUsername(username);
            Assert.assertFalse(uniqueUids.contains(uid)); // check uniqueness
            uniqueUids.add(uid);
        }
    }

    @Test
    public void testUsersHaveSameUidsAsBeforeSomeUserDeletions() {
        List<ByteBuffer> emptySet = Lists.newArrayList();
        List<ByteBuffer> users = (List<ByteBuffer>) addMoreUsers(emptySet,
                manager);
        Map<ByteBuffer, Short> uids = Maps.newHashMap();
        for (ByteBuffer username : users) { // retrieve
            short uid = manager.getUidByUsername(username); // valid uids
            uids.put(username, uid); // after add users
        }
        users = deleteSomeUsers(users, manager);
        uids = Maps.newHashMap();
        for (ByteBuffer username : users) { // retrieve
            short uid = manager.getUidByUsername(username); // valid uids
            uids.put(username, uid); // after delete users
        }
        for (ByteBuffer username : users) {
            short uid = manager.getUidByUsername(username);
            Assert.assertEquals((short) uids.get(username), uid);// check
                                                                 // uniqueness
        }
        AccessManager manager2 = AccessManager.create(current); // simulate
                                                                // server
                                                                // restart by
                                                                // creating new
                                                                // manager
        users = (List<ByteBuffer>) addMoreUsers(users, manager2);
        for (ByteBuffer username : users) { // retrieve
            short uid = manager2.getUidByUsername(username); // valid uids
            uids.put(username, uid); // after add users
        }
        for (ByteBuffer username : users) {
            short uid = manager2.getUidByUsername(username);
            Assert.assertEquals((short) uids.get(username), uid);// check
                                                                 // uniqueness
        }
    }

    @Test
    public void testAllUsersHaveSameUidsAsBeforeServerRestarts() {
        Set<ByteBuffer> emptySet = Sets.newHashSet();
        Set<ByteBuffer> users = (Set<ByteBuffer>) addMoreUsers(emptySet,
                manager);
        Map<ByteBuffer, Short> uids = Maps.newHashMap();
        for (ByteBuffer username : users) { // retrieve valid
            short uid = manager.getUidByUsername(username); // uids after
            uids.put(username, uid); // add users
        }
        AccessManager manager2 = AccessManager.create(current); // simulate
                                                                // server
                                                                // restart by
                                                                // creating new
                                                                // manager
        for (ByteBuffer username : users) {
            short uid = manager2.getUidByUsername(username);
            Assert.assertEquals((short) uids.get(username), uid);
        }
    }

    @Test
    public void testAllUsersHaveSameUidsAsBeforePasswordChange() {
        Set<ByteBuffer> emptySet = Sets.newHashSet();
        Set<ByteBuffer> users = (Set<ByteBuffer>) addMoreUsers(emptySet,
                manager);
        Map<ByteBuffer, Short> uids = Maps.newHashMap();
        for (ByteBuffer username : users) { // retrieve valid
            short uid = manager.getUidByUsername(username); // uids after
            uids.put(username, uid); // add users
        }
        for (ByteBuffer username : users) { // change password
            manager.createUser(username, getSecurePassword());
        }
        for (ByteBuffer username : users) {
            short uid = manager.getUidByUsername(username);
            Assert.assertEquals((short) uids.get(username), uid);
        }
        AccessManager manager2 = AccessManager.create(current); // simulate
                                                                // server
                                                                // restart by
                                                                // creating new
                                                                // manager
        for (ByteBuffer username : users) {
            manager2.createUser(username, getSecurePassword()); // change
                                                                // password
        }
        for (ByteBuffer username : users) {
            short uid = manager2.getUidByUsername(username);
            Assert.assertEquals((short) uids.get(username), uid);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCantRevokeAdmin() {
        manager.deleteUser(toByteBuffer("admin"));
    }

    @Test
    public void testRevokeUser() {
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        manager.createUser(username, password);
        manager.deleteUser(username);
        Assert.assertFalse(manager.isValidUserNamePasswordCombo(username,
                password));
    }

    @Test
    public void testIsValidUsernameAndPassword() {
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        ByteBuffer badpassword = toByteBuffer(TestData.getString() + "bad");
        manager.createUser(username, password);
        Assert.assertTrue(manager.isValidUserNamePasswordCombo(username,
                password));
        Assert.assertFalse(manager.isValidUserNamePasswordCombo(username,
                badpassword));
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
            Assert.assertTrue(manager2.isValidUserNamePasswordCombo(
                    entry.getKey(), entry.getValue()));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCantCreateAccessTokenForInvalidUser() {
        manager.login(toByteBuffer(TestData.getString() + "foo"));
    }

    @Test
    public void testCanCreateAccessTokenForValidUser() {
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        manager.createUser(username, password);
        AccessToken token = manager.login(username);
        Assert.assertTrue(manager.validate(token));
    }

    @Test
    public void testAccessTokenIsNotValidIfServerRestarts() {
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        manager.createUser(username, password);
        AccessToken token = manager.login(username);
        AccessManager manager2 = AccessManager.create(current); // simulate
                                                                // server
                                                                // restart by
                                                                // creating new
                                                                // manager
        Assert.assertFalse(manager2.validate(token));
    }

    @Test
    public void testAccessTokenIsNotValidIfPasswordChanges() {
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        ByteBuffer password2 = getSecurePassword();
        manager.createUser(username, password);
        AccessToken token = manager.login(username);
        manager.createUser(username, password2);
        Assert.assertFalse(manager.validate(token));
    }

    @Test
    public void testAccessTokenIsNotValidIfAccessIsRevoked() {
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        manager.createUser(username, password);
        AccessToken token = manager.login(username);
        manager.deleteUser(username);
        Assert.assertFalse(manager.validate(token));
    }

    @Test
    public void testAccessTokenAutoExpiration() throws InterruptedException {
        manager = AccessManager.createForTesting(current, 60,
                TimeUnit.MILLISECONDS);
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        manager.createUser(username, password);
        AccessToken token = manager.login(username);
        TimeUnit.MILLISECONDS.sleep(60);
        Assert.assertFalse(manager.validate(token));
    }

    @Test
    public void testInvalidateAccessToken() {
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        manager.createUser(username, password);
        AccessToken token = manager.login(username);
        manager.logout(token);
        Assert.assertFalse(manager.validate(token));
    }

    @Test
    public void testTwoAccessTokensForSameUser() {
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        manager.createUser(username, password);
        AccessToken token1 = manager.login(username);
        AccessToken token2 = manager.login(username);
        Assert.assertNotEquals(token1, token2);
        Assert.assertTrue(manager.validate(token1));
        Assert.assertTrue(manager.validate(token2));
    }

    @Test
    public void testInvalidatingOneAccessTokenDoesNotAffectOther() {
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        manager.createUser(username, password);
        AccessToken token1 = manager.login(username);
        AccessToken token2 = manager.login(username);
        manager.logout(token2);
        Assert.assertTrue(manager.validate(token1));
    }

    @Test
    public void testRevokingAccessInvalidatesAllAccessTokens() {
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        manager.createUser(username, password);
        List<AccessToken> tokens = Lists.newArrayList();
        for (int i = 0; i < TestData.getScaleCount(); i++) {
            tokens.add(manager.login(username));
        }
        manager.deleteUser(username);
        for (AccessToken token : tokens) {
            Assert.assertFalse(manager.validate(token));
        }
    }

    @Test
    public void testChangingPasswordInvalidatesAllAccessTokens() {
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        manager.createUser(username, password);
        List<AccessToken> tokens = Lists.newArrayList();
        for (int i = 0; i < TestData.getScaleCount(); i++) {
            tokens.add(manager.login(username));
        }
        manager.createUser(username, getSecurePassword());
        for (AccessToken token : tokens) {
            Assert.assertFalse(manager.validate(token));
        }
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
        while (password == null || !AccessManager.isSecuredPassword(password)) {
            password = toByteBuffer(TestData.getString());
        }
        return password;
    }

    /**
     * Return a collection of unique binary usernames that is
     * added to the specified {@code manager}, which is also a
     * superset of the {@code existingUsers} and newly added
     * usernames.
     * 
     * @param existingUsers
     * @param manager
     * @return the valid usernames
     */
    private static Collection<ByteBuffer> addMoreUsers(
            Collection<ByteBuffer> existingUsers, AccessManager manager) {
        Set<ByteBuffer> usernames = Sets.newHashSet();
        int count = TestData.getScaleCount();
        while (usernames.size() < count) {
            ByteBuffer username = getAcceptableUsername();
            if(!usernames.contains(username)) {
                ByteBuffer password = getSecurePassword();
                manager.createUser(username, password);
                existingUsers.add(username);
                usernames.add(username);
            }
        }
        return existingUsers;
    }

    /**
     * Return a list of binary usernames that is still valid
     * after some usernames in {@code existingUsers} has been
     * randomly deleted from {@code manager}.
     * 
     * @param existingUsers
     * @param manager
     * @return the valid usernames
     */
    private static List<ByteBuffer> deleteSomeUsers(
            List<ByteBuffer> existingUsers, AccessManager manager) {
        java.util.Random rand = new java.util.Random();
        Set<ByteBuffer> removedUsers = Sets.newHashSet();
        int count = rand.nextInt(existingUsers.size());
        for (int i = 0; i < count; i++) {
            ByteBuffer username = existingUsers.get(rand.nextInt(existingUsers
                    .size()));
            removedUsers.add(username);
        }
        for (ByteBuffer username : removedUsers) {
            manager.deleteUser(username);
            existingUsers.remove(username);
        }
        return existingUsers;
    }

}
