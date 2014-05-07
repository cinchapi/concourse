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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
        Assert.assertTrue(manager.validate(username, password));
    }

    @Test
    public void testChangeAdminPassword() {
        ByteBuffer username = ByteBuffer.wrap("admin".getBytes());
        ByteBuffer password = ByteBuffer.wrap("admin".getBytes());
        ByteBuffer newPassword = ByteBuffer.wrap(TestData.getString()
                .getBytes());
        manager.grant(username, newPassword);
        Assert.assertFalse(manager.validate(username, password));
        Assert.assertTrue(manager.validate(username, newPassword));
    }

    @Test
    public void testAddUsers() {
        Map<ByteBuffer, ByteBuffer> users = Maps.newHashMap();
        for (int i = 0; i < TestData.getScaleCount(); i++) {
            ByteBuffer username = getAcceptableUsername();
            ByteBuffer password = getSecurePassword();
            users.put(username, password);
            manager.grant(username, password);
        }
        for (Entry<ByteBuffer, ByteBuffer> entry : users.entrySet()) {
            Assert.assertTrue(manager.validate(entry.getKey(), entry.getValue()));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCantRevokeAdmin() {
        manager.revoke(toByteBuffer("admin"));
    }

    @Test
    public void testRevokeUser() {
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        manager.grant(username, password);
        manager.revoke(username);
        Assert.assertFalse(manager.validate(username, password));
    }

    @Test
    public void testIsValidUsernameAndPassword() {
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        ByteBuffer badpassword = toByteBuffer(TestData.getString() + "bad");
        manager.grant(username, password);
        Assert.assertTrue(manager.validate(username, password));
        Assert.assertFalse(manager.validate(username, badpassword));
    }

    @Test
    public void testDiskSync() {
        Map<ByteBuffer, ByteBuffer> users = Maps.newHashMap();
        for (int i = 0; i < TestData.getScaleCount(); i++) {
            ByteBuffer username = getAcceptableUsername();
            ByteBuffer password = getSecurePassword();
            users.put(username, password);
            manager.grant(username, password);
        }
        AccessManager manager2 = AccessManager.create(current);
        for (Entry<ByteBuffer, ByteBuffer> entry : users.entrySet()) {
            Assert.assertTrue(manager2.validate(entry.getKey(),
                    entry.getValue()));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCantCreateAccessTokenForInvalidUser() {
        manager.authorize(toByteBuffer(TestData.getString() + "foo"));
    }

    @Test
    public void testCanCreateAccessTokenForValidUser() {
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        manager.grant(username, password);
        AccessToken token = manager.authorize(username);
        Assert.assertTrue(manager.validate(token));
    }

    @Test
    public void testAccessTokenIsNotValidIfServerRestarts() {
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        manager.grant(username, password);
        AccessToken token = manager.authorize(username);
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
        manager.grant(username, password);
        AccessToken token = manager.authorize(username);
        manager.grant(username, password2);
        Assert.assertFalse(manager.validate(token));
    }

    @Test
    public void testAccessTokenIsNotValidIfAccessIsRevoked() {
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        manager.grant(username, password);
        AccessToken token = manager.authorize(username);
        manager.revoke(username);
        Assert.assertFalse(manager.validate(token));
    }

    @Test
    public void testAccessTokenAutoExpiration() throws InterruptedException {
        manager = AccessManager.createForTesting(current, 60,
                TimeUnit.MILLISECONDS);
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        manager.grant(username, password);
        AccessToken token = manager.authorize(username);
        TimeUnit.MILLISECONDS.sleep(60);
        Assert.assertFalse(manager.validate(token));
    }

    @Test
    public void testInvalidateAccessToken() {
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        manager.grant(username, password);
        AccessToken token = manager.authorize(username);
        manager.deauthorize(token);
        Assert.assertFalse(manager.validate(token));
    }
    
    @Test
    public void testTwoAccessTokensForSameUser(){
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        manager.grant(username, password);
        AccessToken token1 = manager.authorize(username);
        AccessToken token2 = manager.authorize(username);
        Assert.assertNotEquals(token1, token2);
        Assert.assertTrue(manager.validate(token1));
        Assert.assertTrue(manager.validate(token2));
    }
    
    @Test
    public void testInvalidatingOneAccessTokenDoesNotAffectOther(){
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        manager.grant(username, password);
        AccessToken token1 = manager.authorize(username);
        AccessToken token2 = manager.authorize(username);
        manager.deauthorize(token2);
        Assert.assertTrue(manager.validate(token1));
    }
    
    @Test
    public void testRevokingAccessInvalidatesAllAccessTokens(){
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        manager.grant(username, password);
        List<AccessToken> tokens = Lists.newArrayList();
        for(int i = 0; i < TestData.getScaleCount(); i++){
            tokens.add(manager.authorize(username));
        }
        manager.revoke(username);
        for(AccessToken token : tokens){
            Assert.assertFalse(manager.validate(token));
        }
    }
    
    @Test
    public void testChangingPasswordInvalidatesAllAccessTokens(){
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        manager.grant(username, password);
        List<AccessToken> tokens = Lists.newArrayList();
        for(int i = 0; i < TestData.getScaleCount(); i++){
            tokens.add(manager.authorize(username));
        }
        manager.grant(username, getSecurePassword());
        for(AccessToken token : tokens){
            Assert.assertFalse(manager.validate(token));
        }
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
    
    private static ByteBuffer getAcceptableUsername(){
        ByteBuffer username = null;
        while(username == null || !AccessManager.isAcceptableUsername(username)){
            username = toByteBuffer(TestData.getString());
        }
        return username;
    }
    
    private static ByteBuffer getSecurePassword(){
        ByteBuffer password = null;
        while(password == null || !AccessManager.isSecuredPassword(password)){
            password = toByteBuffer(TestData.getString());
        }
        return password;
    }

}
