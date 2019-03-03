/*
 * Copyright (c) 2013-2019 Cinchapi Inc.
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
package com.cinchapi.concourse.security;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.cinchapi.concourse.server.concurrent.Threads;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.cinchapi.concourse.test.Variables;
import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.ByteBuffers;
import com.cinchapi.concourse.util.Random;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Unit tests for {@link com.cinchapi.concourse.security.UserService}.
 *
 * @author Jeff Nelson
 */
public class UserServiceTest extends ConcourseBaseTest {

    private String current = null;
    private UserService service = null;

    @Rule
    public TestRule watcher = new TestWatcher() {

        @Override
        protected void finished(Description desc) {
            FileSystem.deleteFile(current);
        }

        @Override
        protected void starting(Description desc) {
            current = TestData.DATA_DIR + File.separator + Time.now();
            service = UserService.create(current);

        }
    };

    @Test
    public void testDefaultAdminLogin() {
        ByteBuffer username = ByteBuffer.wrap("admin".getBytes());
        ByteBuffer password = ByteBuffer.wrap("admin".getBytes());
        Assert.assertTrue(service.authenticate(username, password));
    }

    @Test
    public void testChangeAdminPassword() {
        ByteBuffer username = ByteBuffer.wrap("admin".getBytes());
        ByteBuffer password = ByteBuffer.wrap("admin".getBytes());
        ByteBuffer newPassword = getSecurePassword();
        service.setPassword(username, newPassword);
        Assert.assertFalse(service.authenticate(username, password));
        Assert.assertTrue(service.authenticate(username, newPassword));
    }

    @Test
    public void testAddUsers() {
        Map<ByteBuffer, ByteBuffer> users = Maps.newHashMap();
        for (int i = 0; i < TestData.getScaleCount(); i++) {
            ByteBuffer username = getAcceptableUsername();
            ByteBuffer password = getSecurePassword();
            users.put(username, password);
            service.create(username, password, Role.ADMIN);
        }
        for (Entry<ByteBuffer, ByteBuffer> entry : users.entrySet()) {
            Assert.assertTrue(
                    service.authenticate(entry.getKey(), entry.getValue()));
        }
    }

    @Test
    public void testAllUsersHaveUniqueUids() {
        Set<ByteBuffer> emptySet = Sets.newHashSet();
        Set<ByteBuffer> users = (Set<ByteBuffer>) addMoreUsers(emptySet,
                service);
        Set<Short> uniqueUids = Sets.newHashSet();
        for (ByteBuffer username : users) {
            short uid = service.getUserId(username);
            Assert.assertFalse(uniqueUids.contains(uid)); // check uniqueness
            uniqueUids.add(uid);
        }
        UserService service2 = UserService.create(current); // simulate
                                                            // server
                                                            // restart by
                                                            // creating new
                                                            // service
        users = (Set<ByteBuffer>) addMoreUsers(users, service2);
        uniqueUids = Sets.newHashSet();
        for (ByteBuffer username : users) {
            short uid = service2.getUserId(username);
            Assert.assertFalse(uniqueUids.contains(uid)); // check uniqueness
            uniqueUids.add(uid);
        }
    }

    @Test
    public void testAllUsersHaveUniqueUidsAfterSomeUserDeletions() {
        List<ByteBuffer> emptyList = Lists.newArrayList();
        List<ByteBuffer> users = (List<ByteBuffer>) addMoreUsers(emptyList,
                service);
        users = deleteSomeUsers(users, service);
        Set<Short> uniqueUids = Sets.newHashSet();
        for (ByteBuffer username : users) {
            short uid = service.getUserId(username);
            Assert.assertFalse(uniqueUids.contains(uid)); // check uniqueness
            uniqueUids.add(uid);
        }
        UserService service2 = UserService.create(current); // simulate
                                                            // server
                                                            // restart by
                                                            // creating new
                                                            // service
        Variables.register("users", users);
        users = deleteSomeUsers(users, service2);
        Variables.register("users_after_delete", Lists.newArrayList(users));
        users = (List<ByteBuffer>) addMoreUsers(users, service2);
        Variables.register("users_after_add", Lists.newArrayList(users));
        uniqueUids = Sets.newHashSet();
        Variables.register("uniqueUids", uniqueUids);
        for (ByteBuffer username : users) {
            short uid = service2.getUserId(username);
            Variables.register("uid", uid);
            Assert.assertFalse(uniqueUids.contains(uid)); // check uniqueness
            uniqueUids.add(uid);
        }
    }

    @Test
    public void testUsersHaveSameUidsAsBeforeSomeUserDeletions() {
        List<ByteBuffer> emptySet = Lists.newArrayList();
        List<ByteBuffer> users = (List<ByteBuffer>) addMoreUsers(emptySet,
                service);
        Map<ByteBuffer, Short> uids = Maps.newHashMap();
        for (ByteBuffer username : users) { // retrieve
            short uid = service.getUserId(username); // valid uids
            uids.put(username, uid); // after add users
        }
        users = deleteSomeUsers(users, service);
        uids = Maps.newHashMap();
        for (ByteBuffer username : users) { // retrieve
            short uid = service.getUserId(username); // valid uids
            uids.put(username, uid); // after delete users
        }
        for (ByteBuffer username : users) {
            short uid = service.getUserId(username);
            Assert.assertEquals((short) uids.get(username), uid);// check
                                                                 // uniqueness
        }
        UserService service2 = UserService.create(current); // simulate
                                                            // server
                                                            // restart by
                                                            // creating new
                                                            // service
        users = (List<ByteBuffer>) addMoreUsers(users, service2);
        for (ByteBuffer username : users) { // retrieve
            short uid = service2.getUserId(username); // valid uids
            uids.put(username, uid); // after add users
        }
        for (ByteBuffer username : users) {
            short uid = service2.getUserId(username);
            Assert.assertEquals((short) uids.get(username), uid);// check
                                                                 // uniqueness
        }
    }

    @Test
    public void testAllUsersHaveSameUidsAsBeforeServerRestarts() {
        Set<ByteBuffer> emptySet = Sets.newHashSet();
        Set<ByteBuffer> users = (Set<ByteBuffer>) addMoreUsers(emptySet,
                service);
        Map<ByteBuffer, Short> uids = Maps.newHashMap();
        for (ByteBuffer username : users) { // retrieve valid
            short uid = service.getUserId(username); // uids after
            uids.put(username, uid); // add users
        }
        UserService service2 = UserService.create(current); // simulate
                                                            // server
                                                            // restart by
                                                            // creating new
                                                            // service
        for (ByteBuffer username : users) {
            short uid = service2.getUserId(username);
            Assert.assertEquals((short) uids.get(username), uid);
        }
    }

    @Test
    public void testAllUsersHaveSameUidsAsBeforePasswordChange() {
        Set<ByteBuffer> emptySet = Sets.newHashSet();
        Set<ByteBuffer> users = (Set<ByteBuffer>) addMoreUsers(emptySet,
                service);
        Map<ByteBuffer, Short> uids = Maps.newHashMap();
        for (ByteBuffer username : users) { // retrieve valid
            short uid = service.getUserId(username); // uids after
            uids.put(username, uid); // add users
        }
        for (ByteBuffer username : users) { // change password
            service.setPassword(username, getSecurePassword());
        }
        for (ByteBuffer username : users) {
            short uid = service.getUserId(username);
            Assert.assertEquals((short) uids.get(username), uid);
        }
        UserService service2 = UserService.create(current); // simulate
                                                            // server
                                                            // restart by
                                                            // creating new
                                                            // service
        for (ByteBuffer username : users) {
            service2.setPassword(username, getSecurePassword()); // change
                                                                 // password
        }
        for (ByteBuffer username : users) {
            short uid = service2.getUserId(username);
            Assert.assertEquals((short) uids.get(username), uid);
        }
    }

    @Test
    public void testCanDeleteDefaultAdminAccount() {
        service.create(getAcceptableUsername(), getSecurePassword(),
                Role.ADMIN);
        service.delete(toByteBuffer("admin"));
        Assert.assertFalse(service.exists(toByteBuffer("admin")));
    }

    @Test
    public void testRevokeUser() {
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        service.create(username, password, Role.ADMIN);
        service.delete(username);
        Assert.assertFalse(service.authenticate(username, password));
    }

    @Test
    public void testEnableUser() {
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        service.create(username, password, Role.ADMIN);
        service.disable(username);
        service.enable(username);
        Assert.assertTrue(service.isEnabled(username));
    }

    @Test
    public void testDisableUser() {
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        service.create(username, password, Role.ADMIN);
        service.disable(username);
        Assert.assertFalse(service.isEnabled(username));
    }

    @Test
    public void testDisablingUserInvalidatesAllAccessTokens() {
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        service.create(username, password, Role.ADMIN);
        List<AccessToken> tokens = Lists.newArrayList();
        for (int i = 0; i < TestData.getScaleCount(); i++) {
            tokens.add(service.tokens.issue(username));
        }
        service.disable(username);
        for (AccessToken token : tokens) {
            Assert.assertFalse(service.tokens.isValid(token));
        }
    }

    @Test
    public void testNewlyCreatedUserIsEnabled() {
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        service.create(username, password, Role.ADMIN);
        Assert.assertTrue(service.isEnabled(username));
    }

    @Test
    public void testIsValidUsernameAndPassword() {
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        ByteBuffer badpassword = toByteBuffer(TestData.getString() + "bad");
        service.create(username, password, Role.ADMIN);
        Assert.assertTrue(service.authenticate(username, password));
        Assert.assertFalse(service.authenticate(username, badpassword));
    }

    @Test
    public void testDiskSync() {
        Map<ByteBuffer, ByteBuffer> users = Maps.newHashMap();
        for (int i = 0; i < TestData.getScaleCount(); i++) {
            ByteBuffer username = getAcceptableUsername();
            ByteBuffer password = getSecurePassword();
            users.put(username, password);
            service.create(username, password, Role.ADMIN);
        }
        UserService service2 = UserService.create(current);
        for (Entry<ByteBuffer, ByteBuffer> entry : users.entrySet()) {
            Assert.assertTrue(
                    service2.authenticate(entry.getKey(), entry.getValue()));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCantCreateAccessTokenForInvalidUser() {
        service.tokens.issue(toByteBuffer(TestData.getString() + "foo"));
    }

    @Test
    public void testCanCreateAccessTokenForValidUser() {
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        service.create(username, password, Role.ADMIN);
        AccessToken token = service.tokens.issue(username);
        Assert.assertTrue(service.tokens.isValid(token));
    }

    @Test
    public void testAccessTokenIsNotValidIfServerRestarts() {
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        service.create(username, password, Role.ADMIN);
        AccessToken token = service.tokens.issue(username);
        UserService service2 = UserService.create(current); // simulate
                                                            // server
                                                            // restart by
                                                            // creating new
                                                            // service
        Assert.assertFalse(service2.tokens.isValid(token));
    }

    @Test
    public void testAccessTokenIsNotValidIfPasswordChanges() {
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        ByteBuffer password2 = getSecurePassword();
        service.create(username, password, Role.ADMIN);
        AccessToken token = service.tokens.issue(username);
        service.setPassword(username, password2);
        Assert.assertFalse(service.tokens.isValid(token));
    }

    @Test
    public void testAccessTokenIsNotValidIfAccessIsRevoked() {
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        service.create(username, password, Role.ADMIN);
        AccessToken token = service.tokens.issue(username);
        service.delete(username);
        Assert.assertFalse(service.tokens.isValid(token));
    }

    @Test
    public void testAccessTokenAutoExpiration() throws InterruptedException {
        service = UserService.createForTesting(current, 60,
                TimeUnit.MILLISECONDS);
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        service.create(username, password, Role.ADMIN);
        AccessToken token = service.tokens.issue(username);
        TimeUnit.MILLISECONDS.sleep(60);
        Assert.assertFalse(service.tokens.isValid(token));
    }

    @Test
    public void testInvalidateAccessToken() {
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        service.create(username, password, Role.ADMIN);
        AccessToken token = service.tokens.issue(username);
        service.tokens.expire(token);
        Assert.assertFalse(service.tokens.isValid(token));
    }

    @Test
    public void testTwoAccessTokensForSameUser() {
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        service.create(username, password, Role.ADMIN);
        AccessToken token1 = service.tokens.issue(username);
        AccessToken token2 = service.tokens.issue(username);
        Assert.assertNotEquals(token1, token2);
        Assert.assertTrue(service.tokens.isValid(token1));
        Assert.assertTrue(service.tokens.isValid(token2));
    }

    @Test
    public void testInvalidatingOneAccessTokenDoesNotAffectOther() {
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        service.create(username, password, Role.ADMIN);
        AccessToken token1 = service.tokens.issue(username);
        AccessToken token2 = service.tokens.issue(username);
        service.tokens.expire(token2);
        Assert.assertTrue(service.tokens.isValid(token1));
    }

    @Test
    public void testRevokingAccessInvalidatesAllAccessTokens() {
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        service.create(username, password, Role.ADMIN);
        List<AccessToken> tokens = Lists.newArrayList();
        for (int i = 0; i < TestData.getScaleCount(); i++) {
            tokens.add(service.tokens.issue(username));
        }
        service.delete(username);
        for (AccessToken token : tokens) {
            Assert.assertFalse(service.tokens.isValid(token));
        }
    }

    @Test
    public void testChangingPasswordInvalidatesAllAccessTokens() {
        ByteBuffer username = getAcceptableUsername();
        ByteBuffer password = getSecurePassword();
        service.create(username, password, Role.ADMIN);
        List<AccessToken> tokens = Lists.newArrayList();
        for (int i = 0; i < TestData.getScaleCount(); i++) {
            tokens.add(service.tokens.issue(username));
        }
        service.setPassword(username, getSecurePassword());
        for (AccessToken token : tokens) {
            Assert.assertFalse(service.tokens.isValid(token));
        }
    }

    @Test
    public void testEmptyPasswordNotSecure() {
        Assert.assertFalse(
                UserService.isSecurePassword(ByteBuffers.fromString("")));
    }

    @Test
    public void testAllWhitespacePasswordNotSecure() {
        Assert.assertFalse(
                UserService.isSecurePassword(ByteBuffers.fromString("     ")));
    }

    @Test
    public void testUsernameWithWhitespaceNotAcceptable() {
        Assert.assertFalse(UserService
                .isAcceptableUsername(ByteBuffers.fromString("   f  ")));
    }

    @Test
    public void testServiceTokenIsValid() {
        AccessToken token = service.tokens.serviceIssue();
        Assert.assertTrue(service.tokens.isValid(token));
    }

    @Test
    public void testServiceTokenInvalidation() {
        AccessToken token = service.tokens.serviceIssue();
        service.tokens.expire(token);
        Assert.assertFalse(service.tokens.isValid(token));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testServiceTokenNotTiedToUser() {
        AccessToken token = service.tokens.serviceIssue();
        service.getUserId(token);
    }

    @Test
    public void testServiceTokenUsesInvalidUsername() {
        AccessToken token = service.tokens.serviceIssue();
        ByteBuffer username = service.tokens.identify(token);
        Assert.assertFalse(UserService.isAcceptableUsername(username));
    }

    @Test
    public void testServerTokenNotAutoExpire() {
        service = UserService.createForTesting(current, 100,
                TimeUnit.MILLISECONDS);
        AccessToken token = service.tokens.serviceIssue();
        Threads.sleep(100);
        Assert.assertTrue(service.tokens.isValid(token));
    }

    @Test
    public void testUsers() {
        Collection<ByteBuffer> added = addMoreUsers(
                Sets.newHashSet(ByteBuffers.fromString("admin")), service);
        Set<String> existing = added.stream()
                .map(bytes -> ByteBuffers.getString(bytes))
                .collect(Collectors.toSet());
        Assert.assertEquals(existing,
                service.users().stream()
                        .map(bytes -> ByteBuffers.getString(bytes))
                        .collect(Collectors.toSet()));
    }

    @Test
    public void testRecreateDeletedUser() {
        ByteBuffer username = getAcceptableUsername();
        service.create(username, getSecurePassword(), Role.USER);
        service.delete(username);
        service.create(username, getSecurePassword(), Role.ADMIN);
    }

    @Test(expected = SecurityException.class)
    public void testDisabledUserCannotGetToken() {
        ByteBuffer username = getAcceptableUsername();
        service.create(username, getSecurePassword(), Role.USER);
        service.disable(username);
        service.tokens.issue(username);
    }

    @Test
    public void testRenabledUserCanGetToken() {
        ByteBuffer username = getAcceptableUsername();
        service.create(username, getSecurePassword(), Role.USER);
        service.disable(username);
        service.enable(username);
        AccessToken token = service.tokens.issue(username);
        Assert.assertNotNull(token);
    }

    @Test
    public void testChangeRole() {
        ByteBuffer username = getAcceptableUsername();
        service.create(username, getSecurePassword(), Role.ADMIN);
        service.setRole(username, Role.USER);
        Assert.assertEquals(Role.USER, service.getRole(username));
    }

    @Test
    public void testChangingPasswordDoesNotReenableUser() {
        ByteBuffer username = getAcceptableUsername();
        service.create(username, getSecurePassword(), Role.ADMIN);
        service.disable(username);
        service.setPassword(username, getSecurePassword());
        Assert.assertFalse(service.isEnabled(username));
    }

    @Test(expected = IllegalStateException.class)
    public void testCannotDeleteAdminRoleUserIfNoOtherAdminRoleUserExists() {
        ByteBuffer username = getAcceptableUsername();
        service.create(username, getSecurePassword(), Role.ADMIN);
        service.create(getAcceptableUsername(), getSecurePassword(), Role.USER);
        service.delete(toByteBuffer("admin"));
        service.delete(username);
    }

    @Test
    public void testCanDeleteAdminRoleUserIfAnotherAdminRoleUserExists() {
        ByteBuffer username = getAcceptableUsername();
        service.create(username, getSecurePassword(), Role.ADMIN);
        service.create(getAcceptableUsername(), getSecurePassword(),
                Role.ADMIN);
        service.delete(toByteBuffer("admin"));
        service.delete(username);
    }

    @Test(expected = IllegalStateException.class)
    public void testCannotDisableAdminRoleUserIfNoOtherAdminRoleUserExists() {
        ByteBuffer username = getAcceptableUsername();
        service.create(username, getSecurePassword(), Role.ADMIN);
        service.create(getAcceptableUsername(), getSecurePassword(), Role.USER);
        service.delete(toByteBuffer("admin"));
        service.disable(username);
    }

    @Test
    public void testCanDisableAdminRoleUserIfAnotherAdminRoleUserExists() {
        ByteBuffer username = getAcceptableUsername();
        service.create(username, getSecurePassword(), Role.ADMIN);
        service.create(getAcceptableUsername(), getSecurePassword(),
                Role.ADMIN);
        service.delete(toByteBuffer("admin"));
        service.disable(username);
    }

    @Test(expected = IllegalStateException.class)
    public void testCannotChangeRoleOfAdminRoleUserIfNoOtherAdminRoleUserExists() {
        ByteBuffer username = getAcceptableUsername();
        service.create(username, getSecurePassword(), Role.ADMIN);
        service.create(getAcceptableUsername(), getSecurePassword(), Role.USER);
        service.delete(toByteBuffer("admin"));
        service.setRole(username, Role.USER);
    }

    @Test
    public void testCanChangeRoleOfAdminRoleUserIfAnotherAdminRoleUserExists() {
        ByteBuffer username = getAcceptableUsername();
        service.create(username, getSecurePassword(), Role.ADMIN);
        service.create(getAcceptableUsername(), getSecurePassword(),
                Role.ADMIN);
        service.delete(toByteBuffer("admin"));
        service.setRole(username, Role.USER);
    }

    @Test
    public void testRoleOfServiceUserTokenIsAlwaysServiceRole() {
        AccessToken token = service.tokens.serviceIssue();
        ByteBuffer username = service.tokens.identify(token);
        Assert.assertEquals(Role.SERVICE, service.getRole(username));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCannotSetUserRoleToServiceOnCreation() {
        service.create(getAcceptableUsername(), getSecurePassword(),
                Role.SERVICE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCannotSetUserRoleToServiceAfterCreation() {
        ByteBuffer username = getAcceptableUsername();
        service.create(username, getSecurePassword(), Role.USER);
        service.setRole(username, Role.SERVICE);
    }

    @Test
    public void testAdminUserAlwaysHasPermissionRegardlessOfUnderlyingGrant() {
        ByteBuffer username = getAcceptableUsername();
        service.create(username, getSecurePassword(), Role.ADMIN);
        Assert.assertTrue(service.can(username, Permission.READ,
                Random.getSimpleString()));
        Assert.assertTrue(service.can(username, Permission.WRITE,
                Random.getSimpleString()));
    }

    @Test
    public void testNonAdminUserNeverHasPermissionsNotGranted() {
        ByteBuffer username = getAcceptableUsername();
        service.create(username, getSecurePassword(), Role.USER);
        Assert.assertFalse(service.can(username, Permission.READ,
                Random.getSimpleString()));
        Assert.assertFalse(service.can(username, Permission.WRITE,
                Random.getSimpleString()));
    }

    @Test
    public void testGrantPermission() {
        ByteBuffer username = getAcceptableUsername();
        service.create(username, getSecurePassword(), Role.USER);
        String e1 = Random.getSimpleString();
        String e2 = Random.getSimpleString();
        service.grant(username, Permission.READ, e1);
        service.grant(username, Permission.WRITE, e2);
        Assert.assertTrue(service.can(username, Permission.READ, e1));
        Assert.assertFalse(service.can(username, Permission.WRITE, e1));
        Assert.assertTrue(service.can(username, Permission.READ, e2));
        Assert.assertTrue(service.can(username, Permission.WRITE, e2));
        service.grant(username, Permission.READ, e2);
        Assert.assertFalse(service.can(username, Permission.WRITE, e2));
        service.grant(username, Permission.WRITE, e2);
        service.revoke(username, e2);
        Assert.assertTrue(service.can(username, Permission.READ, e1));
        Assert.assertFalse(service.can(username, Permission.WRITE, e1));
        Assert.assertFalse(service.can(username, Permission.READ, e2));
        Assert.assertFalse(service.can(username, Permission.WRITE, e2));
        service.setRole(username, Role.ADMIN);
        Assert.assertTrue(service.can(username, Permission.READ, e1));
        Assert.assertTrue(service.can(username, Permission.WRITE, e1));
        Assert.assertTrue(service.can(username, Permission.READ, e2));
        Assert.assertTrue(service.can(username, Permission.WRITE, e2));
        service.setRole(username, Role.USER);
        Assert.assertTrue(service.can(username, Permission.READ, e1));
        Assert.assertFalse(service.can(username, Permission.WRITE, e1));
        Assert.assertFalse(service.can(username, Permission.READ, e2));
        Assert.assertFalse(service.can(username, Permission.WRITE, e2));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCannotRevokePermissionsForAdminUser() {
        ByteBuffer username = getAcceptableUsername();
        service.create(username, getSecurePassword(), Role.ADMIN);
        service.revoke(username, Random.getSimpleString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCannotRevokePermissionForServiceUser() {
        ByteBuffer username = service.tokens
                .identify(service.tokens.serviceIssue());
        service.revoke(username, Random.getSimpleString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCannotGrantPermissionForServiceUser() {
        ByteBuffer username = service.tokens
                .identify(service.tokens.serviceIssue());
        service.grant(username, Permission.READ, Random.getSimpleString());
    }

    @Test
    public void testServiceUserAlwaysHasPermission() {
        ByteBuffer username = service.tokens
                .identify(service.tokens.serviceIssue());
        Assert.assertTrue(service.can(username, Permission.READ,
                Random.getSimpleString()));
        Assert.assertTrue(service.can(username, Permission.WRITE,
                Random.getSimpleString()));
    }

    @Test
    public void testAdminUserRoleAlwaysHasPermission() {
        ByteBuffer username = getAcceptableUsername();
        service.create(username, getSecurePassword(), Role.ADMIN);
        Assert.assertTrue(service.can(username, Permission.READ,
                Random.getSimpleString()));
        Assert.assertTrue(service.can(username, Permission.WRITE,
                Random.getSimpleString()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCannotCreateUserWithServiceUsername() {
        ByteBuffer username = service.tokens
                .identify(service.tokens.serviceIssue());
        service.create(username, getSecurePassword(), Role.ADMIN);
    }

    @Test
    public void testCannotDeleteUserWithServiceUsername() {
        ByteBuffer username = service.tokens
                .identify(service.tokens.serviceIssue());
        service.delete(username);
        Assert.assertTrue(service.can(username, Permission.READ,
                Random.getSimpleString()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDeletedUserHasNoPermissions() {
        ByteBuffer username = getAcceptableUsername();
        service.create(username, getSecurePassword(), Role.ADMIN);
        service.delete(username);
        Assert.assertFalse(service.can(username, Permission.READ,
                Random.getSimpleString()));
    }

    @Test
    public void testDisabledUserHasPermissions() {
        // NOTE: This is to lock in the invariant that a user's permission alone
        // is not an indicator of access. Access is determined by both the
        // user's 1) permission, and 2) enabled/disable status. This needs to be
        // enforced outside of the user service.
        ByteBuffer username = getAcceptableUsername();
        service.create(username, getSecurePassword(), Role.ADMIN);
        service.disable(username);
        Assert.assertTrue(service.can(username, Permission.READ,
                Random.getSimpleString()));
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
                || !UserService.isAcceptableUsername(username)) {
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
        while (password == null || !UserService.isSecurePassword(password)) {
            password = toByteBuffer(TestData.getString());
        }
        return password;
    }

    /**
     * Return a collection of unique binary usernames that is
     * added to the specified {@code service}, which is also a
     * superset of the {@code existingUsers} and newly added
     * usernames.
     * 
     * @param existingUsers
     * @param service
     * @return the valid usernames
     */
    private static Collection<ByteBuffer> addMoreUsers(
            Collection<ByteBuffer> existingUsers, UserService service) {
        int count = TestData.getScaleCount();
        int added = 0;
        while (added < count) {
            ByteBuffer username = null;
            while (username == null || existingUsers.contains(username)) {
                username = getAcceptableUsername();
            }
            ByteBuffer password = getSecurePassword();
            service.create(username, password, Role.ADMIN);
            existingUsers.add(username);
            ++added;
        }
        return existingUsers;
    }

    /**
     * Return a list of binary usernames that is still valid
     * after some usernames in {@code existingUsers} has been
     * randomly deleted from {@code service}.
     * 
     * @param existingUsers
     * @param service
     * @return the valid usernames
     */
    private static List<ByteBuffer> deleteSomeUsers(
            List<ByteBuffer> existingUsers, UserService service) {
        java.util.Random rand = new java.util.Random();
        Set<ByteBuffer> removedUsers = Sets.newHashSet();
        int count = rand.nextInt(existingUsers.size());
        for (int i = 0; i < count; i++) {
            ByteBuffer username = existingUsers
                    .get(rand.nextInt(existingUsers.size()));
            removedUsers.add(username);
        }
        for (ByteBuffer username : removedUsers) {
            service.delete(username);
            existingUsers.remove(username);
        }
        return existingUsers;
    }

}
