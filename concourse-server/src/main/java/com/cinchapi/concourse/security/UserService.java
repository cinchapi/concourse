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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.common.base.Verify;
import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.annotate.Restricted;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.ByteBuffers;
import com.cinchapi.concourse.util.Random;
import com.cinchapi.concourse.util.Serializables;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Longs;
import edu.emory.mathcs.backport.java.util.Arrays;

/**
 * The {@link UserService} controls access to the server by keeping tracking
 * of valid credentials and handling authentication requests.
 * 
 * @author Jeff Nelson
 */
public class UserService {

    /**
     * The number of hours for which an AccessToken is valid.
     */
    private static final int ACCESS_TOKEN_TTL = 24;

    /**
     * The unit of time for which an AccessToken is valid.
     */
    private static final TimeUnit ACCESS_TOKEN_TTL_UNIT = TimeUnit.HOURS;

    /**
     * The default admin password. If the AccessManager does not have any users,
     * it will automatically create an admin with this password.
     */
    private static final String DEFAULT_ADMIN_PASSWORD = ByteBuffers
            .encodeAsHex(ByteBuffer.wrap("admin".getBytes()));

    /**
     * The default admin username. If the AccessManager does not have any users,
     * it will automatically create an admin with this username.
     */
    private static final String DEFAULT_ADMIN_USERNAME = ByteBuffers
            .encodeAsHex(ByteBuffer.wrap("admin".getBytes()));

    /**
     * The minimum number of character that must be contained in a password.
     */
    private static final int MIN_PASSWORD_LENGTH = 3;

    /**
     * A randomly chosen username for AccessToken's that act as service token's
     * for plugins and other non-user processes. The randomly generated name is
     * chosen so that it is impossible for it to conflict with an actual
     * username, based on the rules that govern valid usernames (e.g. usernames
     * cannot contain spaces)
     */
    private static final String SERVICE_USERNAME_STRING = Random
            .getSimpleString() + " " + Random.getSimpleString();

    /**
     * A {@link ByteBuffer} containing the {@link #SERVICE_USERNAME_STRING}.
     */
    private static final ByteBuffer SERVICE_USERNAME_BYTES = ByteBuffers
            .fromString(SERVICE_USERNAME_STRING);

    /**
     * Hex version of the UTF-8 bytes from {@link SERVICE_USERNAME}.
     */
    private static final String SERVICE_USERNAME_HEX = ByteBuffers
            .encodeAsHex(SERVICE_USERNAME_BYTES);

    /**
     * Create a new AccessManager that stores its credentials in
     * {@code backingStore}.
     * 
     * @param backingStore
     * @return the AccessManager
     */
    public static UserService create(String backingStore) {
        return new UserService(backingStore, ACCESS_TOKEN_TTL,
                ACCESS_TOKEN_TTL_UNIT);
    }

    /**
     * Create an AccessManager with the specified TTL for access tokens. This
     * method should only be used for testing.
     * 
     * @param backingStore
     * @param accessTokenTtl
     * @param accessTokeTtlUnit
     * @return the AccessManager
     */
    @Restricted
    protected static UserService createForTesting(String backingStore,
            int accessTokenTtl, TimeUnit accessTokeTtlUnit) {
        return new UserService(backingStore, accessTokenTtl, accessTokeTtlUnit);
    }

    /**
     * Return {@code true} if {@code username} is in valid format,
     * in which it must not be null or empty, or contain any whitespace.
     * 
     * @param username
     * @return {@code true} if {@code username} is valid format
     */
    @VisibleForTesting
    protected static boolean isAcceptableUsername(ByteBuffer username) {
        CharBuffer chars = ByteBuffers.toCharBuffer(username);
        boolean acceptable = chars.capacity() > 0;
        while (acceptable && chars.hasRemaining()) {
            char c = chars.get();
            if(Character.isWhitespace(c)) {
                acceptable = false;
                break;
            }
        }
        return acceptable;
    }

    /**
     * Return {@code true} if {@code password} is in valid format, which means
     * it meets the following requirements:
     * <ul>
     * <li>{@value #MIN_PASSWORD_LENGTH} or more characters</li>
     * <li>At least one non whitespace character</li>
     * </ul>
     * 
     * @param password
     * @return {@code true} if {@code password} is valid format
     */
    @VisibleForTesting
    protected static boolean isSecurePassword(ByteBuffer password) {
        CharBuffer chars = ByteBuffers.toCharBuffer(password);
        if(password.capacity() >= MIN_PASSWORD_LENGTH) {
            while (chars.hasRemaining()) {
                char c = chars.get();
                if(!Character.isWhitespace(c)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * An interface for managing tokens that are issued to perform actions on
     * behalf of user and service accounts.
     */
    public final AccessTokenManager tokens;

    /**
     * A table in memory that holds the user credentials.
     */
    private final HashBasedTable<Short, String, Object> accounts;

    /**
     * The store where the credentials are serialized on disk.
     */
    private final String backingStore;

    /**
     * A counter that assigns user ids.
     */
    private AtomicInteger counter;

    /**
     * Concurrency control.
     */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Construct a new instance.
     * 
     * @param backingStore
     * @param accessTokenTtl
     * @param accessTokenTtlUnit
     */
    @SuppressWarnings("unchecked")
    private UserService(String backingStore, int accessTokenTtl,
            TimeUnit accessTokenTtlUnit) {
        this.backingStore = backingStore;
        this.tokens = new AccessTokenManager(accessTokenTtl,
                accessTokenTtlUnit);
        if(FileSystem.getFileSize(backingStore) > 0) {
            ByteBuffer bytes = FileSystem.readBytes(backingStore);
            accounts = Serializables.read(bytes, HashBasedTable.class);
            counter = new AtomicInteger(
                    (int) Collections.max(accounts.rowKeySet()));
        }
        else {
            counter = new AtomicInteger(0);
            accounts = HashBasedTable.create();
            // If there are no credentials (which implies this is a new server)
            // add the default admin username/password
            create(ByteBuffers.decodeFromHex(DEFAULT_ADMIN_USERNAME),
                    ByteBuffers.decodeFromHex(DEFAULT_ADMIN_PASSWORD),
                    Role.ADMIN);
        }
    }

    /**
     * Return {@code true} if {@code username} and {@code password} is a valid
     * combination.
     * 
     * @param username
     * @param password
     * @return {@code true} if {@code username}/{@code password} is valid
     */
    public boolean authenticate(ByteBuffer username, ByteBuffer password) {
        lock.readLock().lock();
        try {
            if(exists(username)) {
                short uid = getUserId(username);
                ByteBuffer salt = ByteBuffers.decodeFromHex((String) accounts
                        .get(uid, AccountAttribute.SALT.key()));
                password.rewind();
                password = Passwords.hash(password, salt);
                return ByteBuffers.encodeAsHex(password)
                        .equals((String) accounts.get(uid,
                                AccountAttribute.PASSWORD.key()));
            }
            return false;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Return {@code true} if the user with {@code username} has the
     * {@code permission} in the specified {@code environment}.
     * 
     * @param username
     * @param permission
     * @param environment
     * @return {@code true} if the user with {@code username} can perform the
     *         described action
     */
    public boolean can(ByteBuffer username, Permission permission,
            String environment) {
        lock.readLock().lock();
        try {
            Role role = getRole(username);
            if(role == Role.SERVICE || role == Role.ADMIN) {
                // SERVICE and ADMIN users always have permission to every
                // environment (regardless of their underlying grants, in the
                // case of an ADMIN user)
                return true;
            }
            else {
                User user = getUserStrict(username);
                return user.can(permission, environment);
            }
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Create access to the user identified by {@code username} with
     * {@code password}.
     * <p>
     * <strong>WARNING:</strong> A previous version of this method allowed an
     * existing user's password to be updated. That functionality has been
     * removed. To update a user's password, using the
     * {@link #setPassword(ByteBuffer, ByteBuffer)} method.
     * </p>
     * 
     * @param username
     * @param password
     * @param role
     */
    public void create(ByteBuffer username, ByteBuffer password, Role role) {
        Preconditions.checkArgument(!exists(username), "User already exists");
        Preconditions.checkArgument(!username.equals(SERVICE_USERNAME_BYTES),
                "User already exists");
        Preconditions.checkArgument(isAcceptableUsername(username),
                "Username must not be empty, or contain any whitespace.");
        Preconditions.checkArgument(isSecurePassword(password),
                "Password must not be empty, or have fewer than 3 characters.");
        Preconditions.checkArgument(role != null, "Please specify a role");
        lock.writeLock().lock();
        try {
            short id = (short) counter.incrementAndGet();
            accounts.put(id, AccountAttribute.USERNAME.key(),
                    ByteBuffers.encodeAsHex(username));
            User user = getUser(id);
            user.setPassword(password);
            user.setRole(role);
            user.enable();
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Delete access of the user identified by {@code username}.
     * 
     * @param username
     */
    public void delete(ByteBuffer username) {
        lock.writeLock().lock();
        try {
            User user = getUser(username);
            if(user == null) {
                return;
            }
            else {
                verifyExistsAtLeastOneAdminBesides(user);
                user.delete();
            }
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Check if the user exists and update {@link #ENABLED} flag as false.
     *
     * @param username the username to disable
     */
    public void disable(ByteBuffer username) {
        lock.writeLock().lock();
        try {
            User user = getUserStrict(username);
            verifyExistsAtLeastOneAdminBesides(user);
            user.disable();
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Updates {@link #ENABLED} flag for the user as true in credentials table.
     *
     * @param username the username to enable
     */
    public void enable(ByteBuffer username) {
        lock.writeLock().lock();
        try {
            User user = getUserStrict(username);
            user.enable();
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Return {@code true} if {@code username} exists in {@link #backingStore}.
     * 
     * @param username
     * @return {@code true} if {@code username} exists in {@link #backingStore}
     */
    public boolean exists(ByteBuffer username) {
        lock.readLock().lock();
        try {
            return getUser(username) != null;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Execute a routine on all of the usernames known to this service.
     * 
     * @param consumer
     */
    public void forEachUser(Consumer<ByteBuffer> consumer) {
        accounts.rowKeySet().forEach(id -> {
            ByteBuffer username = ByteBuffers.decodeFromHex(
                    (String) accounts.get(id, AccountAttribute.USERNAME.key()));
            consumer.accept(username);
        });
    }

    /**
     * Return the role for the account associated with the specified
     * {@code username}.
     * 
     * @param username
     * @return the role
     */
    public Role getRole(ByteBuffer username) {
        lock.readLock().lock();
        try {
            if(ByteBuffers.encodeAsHex(username).equals(SERVICE_USERNAME_HEX)) {
                return Role.SERVICE;
            }
            else {
                User user = getUserStrict(username);
                return user.role();
            }
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Grant the {@code permission} in {@code environment} to the user with
     * {@code username}.
     * <p>
     * <strong>NOTE:</strong: A service user cannot receive an explicit
     * permission grant.
     * </p>
     * 
     * @param username
     * @param permission
     * @param environment
     */
    public void grant(ByteBuffer username, Permission permission,
            String environment) {
        lock.writeLock().lock();
        try {
            Verify.thatArgument(getRole(username) != Role.SERVICE,
                    "Cannot grant a permission to a service user");
            User user = getUserStrict(username);
            user.grant(permission, environment);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Return {@code true} if {@code username} exists and is enabled.
     * 
     * @param username the username to check
     * @return {@code true} if {@code username} exists and is enabled
     */
    public boolean isEnabled(ByteBuffer username) {
        lock.readLock().lock();
        try {
            User user = getUserStrict(username);
            return user.isEnabled();
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Revoke any permission that the user with {@code username} has to
     * {@code environment}.
     * 
     * @param username
     * @param environment
     */
    public void revoke(ByteBuffer username, String environment) {
        lock.writeLock().lock();
        try {
            Role role = getRole(username);
            if(role == Role.SERVICE) {
                throw new IllegalArgumentException(
                        "Cannot revoke permissions for a service user");
            }
            else if(role == Role.ADMIN) {
                throw new IllegalArgumentException(
                        "Cannot revoke permissions for an ADMIN user. Please downgrade the user's role and try again");
            }
            else {
                User user = getUserStrict(username);
                user.revoke(environment);
            }
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Set the password associated with the
     * 
     * @param username
     * @param password
     */
    public void setPassword(ByteBuffer username, ByteBuffer password) {
        Preconditions.checkArgument(isSecurePassword(password),
                "Password must not be empty, or have fewer than 3 characters.");
        lock.writeLock().lock();
        try {
            User user = getUserStrict(username);
            user.setPassword(password);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Set the role for the account with the specified {@code username}.
     * 
     * @param username
     * @param role
     */
    public void setRole(ByteBuffer username, Role role) {
        lock.writeLock().lock();
        try {
            User user = getUserStrict(username);
            if(role != Role.ADMIN) {
                verifyExistsAtLeastOneAdminBesides(user);
            }
            user.setRole(role);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Return the uid of the user associated with {@code token}.
     * 
     * @param token
     * @return the uid
     */
    @VisibleForTesting
    protected short getUserId(AccessToken token) {
        lock.readLock().lock();
        try {
            ByteBuffer username = tokens.identify(token);
            User user = getUser(username);
            if(user != null) {
                return user.id();
            }
            else {
                // Check to see if the token is associated with a service
                // account in which case we should throw an
                // IllegalArgumentException to differentiate from when there is
                // an internal state error.
                String hex = ByteBuffers.encodeAsHex(tokens.identify(token));
                if(hex.equals(SERVICE_USERNAME_HEX)) {
                    throw new IllegalArgumentException(
                            "The specified token is associated with a service and not a user");
                }
                else {
                    throw new IllegalStateException(
                            "An valid access token was presented for a user that does not exist");
                }
            }
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Return the uid of the user identified by {@code username}.
     * 
     * @param username
     * @return the uid
     */
    @VisibleForTesting
    protected short getUserId(ByteBuffer username) {
        lock.readLock().lock();
        try {
            Preconditions.checkArgument(exists(username),
                    "The specified user does not exist");
            Map<Short, Object> credsCol = accounts
                    .column(AccountAttribute.USERNAME.key());
            for (Map.Entry<Short, Object> creds : credsCol.entrySet()) {
                String value = (String) creds.getValue();
                if(value.equals(ByteBuffers.encodeAsHex(username))) {
                    return creds.getKey();
                }
            }
            return -1; // suppress compiler error
                       // but this statement will
                       // never actually execute
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Insert credential with {@code username}, {@code password},
     * and {@code salt} into the memory store.
     * 
     * @param username
     * @param password
     * @param salt
     */
    protected void insertFromLegacy(ByteBuffer username, ByteBuffer password,
            ByteBuffer salt) {
        // NOTE: This method exists for the LegacyAccessManager to insert
        // credentials into this Access Manager. As such, the direct insertions
        // into the #accounts table are intentionally included in this
        // implementation.
        lock.writeLock().lock();
        try {
            short id = (short) counter.incrementAndGet();
            accounts.put(id, AccountAttribute.USERNAME.key(),
                    ByteBuffers.encodeAsHex(username));
            accounts.put(id, AccountAttribute.PASSWORD.key(),
                    ByteBuffers.encodeAsHex(password));
            accounts.put(id, AccountAttribute.SALT.key(),
                    ByteBuffers.encodeAsHex(salt));
            flush();
            User user = getUser(id);
            user.setRole(Role.ADMIN);
            user.enable();
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Return a {@link Set} containing all the registered usernames.
     * 
     * @return the usernames
     */
    @VisibleForTesting
    protected Set<ByteBuffer> users() {
        Set<ByteBuffer> users = Sets.newLinkedHashSet();
        forEachUser(username -> users.add(username));
        return users;
    }

    /**
     * Check to see if there exists at least one admin besides the specified
     * {@code user}.
     * 
     * @param user the {@link User} to exclude from the search.
     * @return {@code true} if there is an admin besides the {@code user}
     */
    private boolean existsAtLeastOneAdminBesides(User user) {
        lock.readLock().lock();
        try {
            for (ByteBuffer username : users()) {
                if(username.equals(user.username())) {
                    continue;
                }
                else if(getRole(username) == Role.ADMIN) {
                    return true;
                }
                else {
                    continue;
                }
            }
            return false;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Sync any changes made in the memory store to disk.
     */
    private void flush() {
        // TODO take a backup in order to make this ACID durable...
        FileChannel channel = FileSystem.getFileChannel(backingStore);
        try {
            channel.position(0);
            Serializables.write(accounts, channel);
        }
        catch (IOException e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
        finally {
            FileSystem.closeFileChannel(channel);
        }
    }

    /**
     * Return the {@link User} with the specified {@code username}
     * 
     * @param username
     * @return the {@link User}
     */
    @Nullable
    private User getUser(ByteBuffer username) {
        lock.readLock().lock();
        try {
            Map<Short, Object> usernames = accounts
                    .column(AccountAttribute.USERNAME.key());
            String seeking = ByteBuffers.encodeAsHex(username);
            for (Entry<Short, Object> profile : usernames.entrySet()) {
                String stored = (String) profile.getValue();
                if(seeking.equals(stored)) {
                    return new User(profile.getKey());
                }
            }
            return null;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Return the {@link User} with the specified {@code id}.
     * 
     * @param id
     * @return the {@link User} object
     */
    @Nullable
    private User getUser(short id) {
        lock.readLock().lock();
        try {
            if(accounts.containsRow(id)) {
                return new User(id);
            }
            else {
                return null;
            }
        }
        finally {
            lock.readLock().unlock();
        }
    }

    private User getUserStrict(ByteBuffer username) {
        User user = getUser(username);
        if(user == null) {
            throw new IllegalArgumentException(
                    "The specified user does not exist");
        }
        else {
            return user;
        }
    }

    /**
     * Throw an {@link IllegalStateException} if
     * {@link #existsAtLeastOneAdminBesides(User)} is false.
     * 
     * @param user
     */
    private void verifyExistsAtLeastOneAdminBesides(User user) {
        if(!existsAtLeastOneAdminBesides(user)) {
            throw new IllegalStateException("The action cannot be performed "
                    + "because doing so would leave no other ADMIN users.");
        }
    }

    /**
     * The {@link AccessTokenManager} handles the work necessary to create,
     * validate and delete AccessTokens for the {@link UserService}.
     * 
     * @author Jeff Nelson
     */
    public final class AccessTokenManager {

        // NOTE: This class does not define #hasCode() or #equals() because the
        // defaults are the desired behaviour.

        /**
         * The collection of currently valid tokens is maintained as a cache
         * mapping from a raw AccessToken to an AccessTokenWrapper. Each raw
         * AccessToken is unique and "equal" to its corresponding wrapper, which
         * contains metadata about the user and timestamp associated with the
         * access token.
         */
        private final Cache<AccessToken, AccessTokenWrapper> active;
        private final ReadWriteLock lock = new ReentrantReadWriteLock();

        private final SecureRandom srand = new SecureRandom();

        /**
         * Construct a new instance.
         * 
         * @param accessTokenTtl
         * @param accessTokenTtlUnit
         */
        private AccessTokenManager(int accessTokenTtl,
                TimeUnit accessTokenTtlUnit) {
            this.active = CacheBuilder.newBuilder()
                    .expireAfterWrite(accessTokenTtl, accessTokenTtlUnit)
                    .removalListener(
                            new RemovalListener<AccessToken, AccessTokenWrapper>() {

                                @Override
                                public void onRemoval(
                                        RemovalNotification<AccessToken, AccessTokenWrapper> notification) {
                                    AccessToken token = notification.getKey();
                                    AccessTokenWrapper wrapper = notification
                                            .getValue();
                                    if(notification.wasEvicted()
                                            && wrapper.isServiceToken()) {
                                        active.put(token, wrapper);
                                    }

                                }
                            })
                    .build();

        }

        /**
         * Return a list of strings, each of which describes a currently
         * existing
         * access token.
         * 
         * @return a list of token descriptions
         */
        public List<String> describeActiveSessions() {
            UserService.this.lock.readLock().lock();
            lock.readLock().lock();
            try {
                List<String> sessions = Lists.newArrayList();
                List<AccessTokenWrapper> active = Lists
                        .newArrayList(this.active.asMap().values());
                Collections.sort(active);
                active.forEach(token -> sessions.add(token.getDescription()));
                return sessions;
            }
            finally {
                lock.readLock().unlock();
                UserService.this.lock.readLock().unlock();
            }
        }

        /**
         * Invalidate {@code token} if it exists.
         * 
         * @param token
         */
        public void expire(AccessToken token) {
            UserService.this.lock.readLock().lock();
            lock.writeLock().lock();
            try {
                active.invalidate(token);
            }
            finally {
                lock.writeLock().unlock();
                UserService.this.lock.readLock().unlock();

            }

        }

        /**
         * Invalidate any and all tokens that exist for {@code username}.
         * 
         * @param username
         */
        public void expireAll(ByteBuffer username) {
            UserService.this.lock.readLock().lock();
            lock.writeLock().lock();
            try {
                User user = getUserStrict(username);
                for (AccessToken token : active.asMap().keySet()) {
                    if(active.getIfPresent(token).getUsername()
                            .equals(user.usernameAsHex())) {
                        active.invalidate(token);
                    }
                }
            }
            finally {
                lock.writeLock().unlock();
                UserService.this.lock.readLock().unlock();
            }
        }

        /**
         * Return the username associated with the valid {@code token}.
         * 
         * @param token
         * @return the username if {@code token} is valid
         */
        public ByteBuffer identify(AccessToken token) {
            UserService.this.lock.readLock().lock();
            lock.readLock().lock();
            try {
                Preconditions.checkArgument(isValid(token),
                        "Access token is no longer invalid.");
                String username = active.getIfPresent(token).getUsername();
                if(!Strings.isNullOrEmpty(username)) {
                    return ByteBuffers.decodeFromHex(username);
                }
                else {
                    throw new IllegalArgumentException(
                            "Token is no longer valid");
                }
            }
            finally {
                lock.readLock().unlock();
                UserService.this.lock.readLock().unlock();
            }
        }

        /**
         * Add and return a new access token for {@code username}.
         * 
         * @param username
         * @return the AccessToken
         */
        public AccessToken issue(ByteBuffer username) {
            UserService.this.lock.readLock().lock();
            lock.writeLock().lock();
            try {
                String hex = ByteBuffers.encodeAsHex(username);
                if(!hex.equals(SERVICE_USERNAME_HEX)) {
                    // Ensure tokens are only being issued for real users that
                    // are enabled
                    User user = getUserStrict(username);
                    hex = user.usernameAsHex();
                    if(!user.isEnabled()) {
                        throw new SecurityException(
                                "Cannot issue a token for a user whose access has been disabled");
                    }
                }
                long timestamp = Time.now();
                StringBuilder sb = new StringBuilder();
                sb.append(username);
                sb.append(srand.nextLong());
                sb.append(timestamp);
                AccessToken token = new AccessToken(ByteBuffer.wrap(Hashing
                        .sha256().hashUnencodedChars(sb.toString()).asBytes()));
                AccessTokenWrapper wapper = AccessTokenWrapper.create(token,
                        hex, timestamp);
                active.put(token, wapper);
                return token;
            }
            finally {
                lock.writeLock().unlock();
                UserService.this.lock.readLock().unlock();
            }
        }

        /**
         * Return {@code true} if {@code token} is valid.
         * 
         * @param token
         * @return {@code true} if {@code token} is valid
         */
        public boolean isValid(AccessToken token) {
            UserService.this.lock.readLock().lock();
            lock.readLock().lock();
            try {
                boolean valid = active.getIfPresent(token) != null;
                if(!valid) {
                    // If the token is invalid, force cleanup of the cache to
                    // trigger the cache removal listener that detects when
                    // service token's have expired and regenerates them.
                    active.cleanUp();
                    valid = active.getIfPresent(token) != null;
                }
                return valid;
            }
            finally {
                lock.readLock().unlock();
                UserService.this.lock.readLock().unlock();
            }
        }

        /**
         * Return a new service token.
         * 
         * <p>
         * A service token is an {@link AccessToken} that is not associated with
         * an
         * actual user, but is instead generated based on the
         * {@link #SERVICE_USERNAME_STRING} and can be assigned to a non-user
         * service
         * or
         * process.
         * </p>
         * <p>
         * Service tokens do not expire!
         * </p>
         * 
         * @return the new service token
         */
        public AccessToken serviceIssue() {
            return issue(SERVICE_USERNAME_BYTES);
        }
    }

    /**
     * An {@link AccessTokenWrapper} associates metadata with an
     * {@link AccessToken}. This data isn't stored directly with the access
     * token because it would provide unnecessary bloat when the token is
     * transferred between client and server, so we use a wrapper on the server
     * side to assist with certain permissions based operations.
     * <p>
     * <strong>NOTE:</strong> The {@link #hashCode} and {@link #equals(Object)}
     * functions only take the wrapped token into account so that objects in
     * this class can be considered to the raw tokens they wrap for the purpose
     * of collection storage.
     * </p>
     * 
     * @author Jeff Nelson
     */
    private static class AccessTokenWrapper
            implements Comparable<AccessTokenWrapper> {

        /**
         * The formatter that is used to when constructing a human readable
         * description of the access token.
         */
        private static final DateTimeFormatter DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
                .appendMonthOfYearShortText().appendLiteral(" ")
                .appendDayOfMonth(1).appendLiteral(", ").appendYear(4, 4)
                .appendLiteral(" at ").appendHourOfDay(1).appendLiteral(":")
                .appendMinuteOfHour(2).appendLiteral(":")
                .appendSecondOfMinute(2).appendLiteral(" ")
                .appendHalfdayOfDayText().toFormatter();

        /**
         * Create a new {@link AccessTokenWrapper} that wraps {@code token} for
         * {@code username} at {@code timestamp}.
         * 
         * @param token
         * @param username
         * @param timestamp
         * @return the AccessTokenWrapper
         */
        public static AccessTokenWrapper create(AccessToken token,
                String username, long timestamp) {
            return new AccessTokenWrapper(token, username, timestamp);
        }

        private final long timestamp;
        private final AccessToken token;
        private final String username; // hex

        /**
         * Construct a new instance.
         * 
         * @param token
         * @param username
         * @param timestamp
         */
        private AccessTokenWrapper(AccessToken token, String username,
                long timestamp) {
            this.token = token;
            this.username = username;
            this.timestamp = timestamp;
        }

        @Override
        public int compareTo(AccessTokenWrapper o) {
            return Longs.compare(timestamp, o.timestamp);
        }

        @Override
        public boolean equals(Object obj) {
            if(obj instanceof AccessTokenWrapper) {
                return token.equals(((AccessTokenWrapper) obj).token);
            }
            return false;
        }

        /**
         * Return the wrapped access token.
         * 
         * @return the token.
         */
        @SuppressWarnings("unused")
        public AccessToken getAccessToken() {
            return token;
        }

        /**
         * Return a human readable description of the access token.
         * 
         * @return the description
         */
        public String getDescription() {
            String uname = ByteBuffers
                    .getString(ByteBuffers.decodeFromHex(username));
            uname = uname.equals(SERVICE_USERNAME_STRING) ? "BACKGROUND SERVICE"
                    : uname;
            return uname + " logged in since " + Timestamp.fromMicros(timestamp)
                    .getJoda().toString(DATE_TIME_FORMATTER);
        }

        /**
         * Return the timestamp that is associated with the wrapped access
         * token.
         * 
         * @return the associated timestamp
         */
        @SuppressWarnings("unused")
        public long getTimestamp() {
            return timestamp;
        }

        /**
         * Return the username that is represented by the wrapped access token.
         * 
         * @return the associated username
         */
        public String getUsername() {
            return username;
        }

        @Override
        public int hashCode() {
            return Objects.hash(token);
        }

        /**
         * Return {@code true} if the wrapped {@link AccessToken} is a service
         * token.
         * 
         * @return a boolean indicates if the token is a service token
         */
        public boolean isServiceToken() {
            return username.equals(SERVICE_USERNAME_HEX);
        }

        @Override
        public String toString() {
            return token.toString();
        }
    }

    /**
     * The attributes that are contained in the {@link #accounts} table.
     *
     * @author Jeff Nelson
     */
    private enum AccountAttribute {

        /**
         * The column that contains a boolean which indicates if a user is
         * enabled or not {@link #accounts} table(When a user is created, its
         * enabled by default).
         */
        ENABLED("user_enabled"),

        /**
         * The column that contains a user's password in the {@link #accounts}
         * table.
         */
        PASSWORD("password"),

        /**
         * The column that contain's a user's role in the {@link #accounts}
         * table.
         */
        ROLE("role"),

        /**
         * The column that contains a user's salt in the {@link #accounts}
         * table.
         */
        SALT("salt"),

        /**
         * The column that contains a user's username in the {@link #accounts}
         * table.
         */
        USERNAME("username"),

        /**
         * The column that contains a mapping with the user's permission grants.
         * That mapping is of an environment name to the permission the user
         * contains in that environment.
         */
        PERMISSIONS("permission");

        /**
         * Return a list of all the {@link AccountAttribute account attributes}.
         * 
         * @return the account attributes
         */
        @SuppressWarnings("unchecked")
        public static List<AccountAttribute> all() {
            return Arrays.asList(AccountAttribute.values());
        }

        private final String key;

        /**
         * Construct a new instance.
         * 
         * @param key
         */
        AccountAttribute(String key) {
            this.key = key;
        }

        public String key() {
            return key;
        }

        @Override
        public String toString() {
            return key();
        }
    }

    /**
     * A read-through encapsulation of all the data stored for a user in the
     * {@link #accounts} table.
     *
     * @author Jeff Nelson
     */
    private class User {

        /**
         * The primary key of the row that contain's the user's data in the
         * {@link #accounts} table
         */
        private final short id;

        /**
         * Construct a new instance.
         * 
         * @param id
         */
        private User(short id) {
            this.id = id;
        }

        /**
         * Return {@code true} if this {@link User} has the {@code permission}
         * in the specified {@code environment}.
         * 
         * @param permission
         * @param environment
         * @return {@code true} of user can perform the described action
         */
        @SuppressWarnings("unchecked")
        private boolean can(Permission permission, String environment) {
            Map<String, Object> permissions = MoreObjects.firstNonNull(
                    (Map<String, Object>) accounts.get(id,
                            AccountAttribute.PERMISSIONS.key()),
                    ImmutableMap.of());
            Permission granted = (Permission) permissions.get(environment);
            if(granted == null) {
                return false;
            }
            else {
                switch (granted) {
                case WRITE:
                    // NOTE: A user with WRITE permission can also read, so this
                    // case is trivially true. However, an explicit return
                    // condition is used to future proof in case more
                    // permissions are added.
                    return permission == Permission.READ
                            || permission == Permission.WRITE;
                case READ:
                    return permission == Permission.READ;
                default:
                    throw new IllegalStateException(
                            "Unknown permission " + granted);
                }
            }
        }

        /**
         * Delete the user.
         */
        private void delete() {
            tokens.expireAll(username());
            AccountAttribute.all()
                    .forEach(attribute -> accounts.remove(id, attribute.key()));
            flush();
        }

        /**
         * Disable the user.
         */
        private void disable() {
            tokens.expireAll(username());
            accounts.put(id, AccountAttribute.ENABLED.key(), false);
            flush();
        }

        /**
         * Enable the user.
         */
        private void enable() {
            tokens.expireAll(username());
            accounts.put(id, AccountAttribute.ENABLED.key(), true);
            flush();
        }

        /**
         * Return the user's id.
         * 
         * @return the id
         */
        private short id() {
            return id;
        }

        /**
         * Return {@code true} if this user is enabled.
         * 
         * @return {@code true} if this user is enabled
         */
        private boolean isEnabled() {
            Object enabled = accounts.get(id, AccountAttribute.ENABLED.key());
            if(enabled == null) {
                enabled = true;
                accounts.put(id, AccountAttribute.ENABLED.key(), enabled);
            }
            return (boolean) enabled;
        }

        /**
         * Grant the {@code permission} in {@code environment} to this
         * {@link User}
         * 
         * @param permission
         * @param environment
         */
        @SuppressWarnings("unchecked")
        private void grant(Permission permission, String environment) {
            Map<String, Permission> permissions = (Map<String, Permission>) accounts
                    .get(id, AccountAttribute.PERMISSIONS.key());
            if(permissions == null) {
                permissions = Maps.newHashMap();
                accounts.put(id, AccountAttribute.PERMISSIONS.key(),
                        permissions);
            }
            permissions.put(environment, permission);
            flush();
        }

        /**
         * Revoke any of the user's permissions to the {@code environment}.
         * 
         * @param environment
         */
        @SuppressWarnings("unchecked")
        private void revoke(String environment) {
            Map<String, Object> permissions = (Map<String, Object>) accounts
                    .get(id, AccountAttribute.PERMISSIONS.key());
            if(permissions != null) {
                permissions.remove(environment);
            }
        }

        /**
         * Return the user's {@link Role role}.
         * 
         * @return the user's role
         */
        private Role role() {
            Integer ordinal = (Integer) accounts.get(id,
                    AccountAttribute.ROLE.key());
            if(ordinal != null) {
                Role role = Role.values()[ordinal];
                return role;
            }
            else {
                throw new IllegalStateException(
                        "The specified user does not have a role");
            }
        }

        /**
         * Set the user's password
         * 
         * @param password
         */
        private void setPassword(ByteBuffer password) {
            tokens.expireAll(username());
            ByteBuffer salt = Passwords.getSalt();
            password = Passwords.hash(password, salt);
            accounts.put(id, AccountAttribute.SALT.key(),
                    ByteBuffers.encodeAsHex(salt));
            accounts.put(id, AccountAttribute.PASSWORD.key(),
                    ByteBuffers.encodeAsHex(password));
            flush();
        }

        /**
         * Set the user's {@link Role role}.
         * 
         * @param role
         */
        private void setRole(Role role) {
            Preconditions.checkArgument(role != Role.SERVICE,
                    "Cannot assign the SERVICE role to a user account");
            tokens.expireAll(username());
            accounts.put(id, AccountAttribute.ROLE.key(), role.ordinal());
            flush();
        }

        /**
         * Return the user's username as a {@link ByteBuffer}
         * 
         * @return the username
         */
        private ByteBuffer username() {
            return ByteBuffers.decodeFromHex(usernameAsHex());
        }

        /**
         * Return the user's username as a hex string.
         * 
         * @return the username
         */
        private String usernameAsHex() {
            return (String) accounts.get(id, AccountAttribute.USERNAME.key());
        }

    }
}
