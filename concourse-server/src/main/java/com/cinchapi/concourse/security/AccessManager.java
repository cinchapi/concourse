/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.StampedLock;

import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;

import static com.google.common.base.Preconditions.*;

import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.annotate.Restricted;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.ByteBuffers;
import com.cinchapi.concourse.util.Random;
import com.cinchapi.concourse.util.Serializables;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Longs;

/**
 * The {@link AccessManager} controls access to the server by keeping tracking
 * of valid credentials and handling authentication requests.
 * 
 * @author Jeff Nelson
 */
public class AccessManager {

    /**
     * Create a new AccessManager that stores its credentials in
     * {@code backingStore}.
     * 
     * @param backingStore
     * @return the AccessManager
     */
    public static AccessManager create(String backingStore) {
        return new AccessManager(backingStore, ACCESS_TOKEN_TTL,
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
    protected static AccessManager createForTesting(String backingStore,
            int accessTokenTtl, TimeUnit accessTokeTtlUnit) {
        return new AccessManager(backingStore, accessTokenTtl,
                accessTokeTtlUnit);
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
     * The column that contains a boolean which indicates if a user is enabled
     * or not {@link #credentials} table(When a user is created, its enabled by
     * default).
     */
    private static final String ENABLED = "user_enabled";

    /**
     * The minimum number of character that must be contained in a password.
     */
    private static final int MIN_PASSWORD_LENGTH = 3;

    /**
     * The column that contains a user's password in the {@link #credentials}
     * table.
     */
    private static final String PASSWORD_KEY = "password";

    /**
     * The column that contains a user's salt in the {@link #credentials} table.
     */
    private static final String SALT_KEY = "salt";

    /**
     * A randomly chosen username for AccessToken's that act as service token's
     * for plugins and other non-user processes. The randomly generated name is
     * chosen so that it is impossible for it to conflict with an actual
     * username, based on the rules that govern valid usernames (e.g. usernames
     * cannot contain spaces)
     */
    private static final String SERVICE_USERNAME = Random.getSimpleString()
            + " " + Random.getSimpleString();

    /**
     * The column that contains a user's username in the {@link #credentials}
     * table.
     */
    private static final String USERNAME_KEY = "username";

    /**
     * The store where the credentials are serialized on disk.
     */
    private final String backingStore;

    /**
     * A counter that assigns user ids.
     */
    private AtomicInteger counter;

    /**
     * A table in memory that holds the user credentials.
     */
    private final HashBasedTable<Short, String, Object> credentials;

    /**
     * Concurrency control.
     */
    private final StampedLock lock = new StampedLock();

    /**
     * Handles access tokens.
     */
    private final AccessTokenManager tokenManager;

    /**
     * Construct a new instance.
     * 
     * @param backingStore
     * @param accessTokenTtl
     * @param accessTokenTtlUnit
     */
    @SuppressWarnings("unchecked")
    private AccessManager(String backingStore, int accessTokenTtl,
            TimeUnit accessTokenTtlUnit) {
        this.backingStore = backingStore;
        this.tokenManager = AccessTokenManager.create(accessTokenTtl,
                accessTokenTtlUnit);
        if(FileSystem.getFileSize(backingStore) > 0) {
            ByteBuffer bytes = FileSystem.readBytes(backingStore);
            credentials = Serializables.read(bytes, HashBasedTable.class);
            counter = new AtomicInteger(
                    (int) Collections.max(credentials.rowKeySet()));
        }
        else {
            counter = new AtomicInteger(0);
            credentials = HashBasedTable.create();
            // If there are no credentials (which implies this is a new server)
            // add the default admin username/password
            createUser(ByteBuffers.decodeFromHex(DEFAULT_ADMIN_USERNAME),
                    ByteBuffers.decodeFromHex(DEFAULT_ADMIN_PASSWORD));
        }
    }

    /**
     * Create access to the user identified by {@code username} with
     * {@code password}.
     * 
     * <p>
     * If the existing user simply changes the password, the new auto-generated
     * id will not be generated and this username still has the same uid as the
     * time it has been assigned when this {@link AccessManager} is
     * instantiated.
     * </p>
     * 
     * @param username
     * @param password
     */
    public void createUser(ByteBuffer username, ByteBuffer password) {
        Preconditions.checkArgument(isAcceptableUsername(username),
                "Username must not be empty, or contain any whitespace.");
        Preconditions.checkArgument(isSecurePassword(password),
                "Password must not be empty, or have fewer than 3 characters.");
        long stamp = lock.writeLock();
        try {
            ByteBuffer salt = Passwords.getSalt();
            password = Passwords.hash(password, salt);
            boolean enabled = true;
            insert0(username, password, salt, enabled);
            tokenManager.deleteAllUserTokens(ByteBuffers.encodeAsHex(username));
            diskSync();
        }
        finally {
            lock.unlockWrite(stamp);
        }
    }

    /**
     * Delete access of the user identified by {@code username}.
     * 
     * @param username
     */
    public void deleteUser(ByteBuffer username) {
        long stamp = lock.writeLock();
        try {
            String hex = ByteBuffers.encodeAsHex(username);
            checkArgument(!hex.equals(DEFAULT_ADMIN_USERNAME),
                    "Cannot revoke access for the admin user!");
            short uid = getUidByUsername0(username);
            credentials.remove(uid, USERNAME_KEY);
            credentials.remove(uid, PASSWORD_KEY);
            credentials.remove(uid, SALT_KEY);
            credentials.remove(uid, ENABLED);
            tokenManager.deleteAllUserTokens(hex);
            diskSync();
        }
        finally {
            lock.unlockWrite(stamp);
        }
    }

    /**
     * Return a list of strings, each of which describes a currently existing
     * access token.
     * 
     * @return a list of token descriptions
     */
    public List<String> describeAllAccessTokens() {
        List<String> sessions = Lists.newArrayList();
        List<AccessTokenWrapper> tokens = Lists
                .newArrayList(tokenManager.tokens.asMap().values());
        Collections.sort(tokens);
        for (AccessTokenWrapper token : tokenManager.tokens.asMap().values()) {
            sessions.add(token.getDescription());
        }
        return sessions;
    }

    /**
     * Check if the user exists and updates {@link #ENABLED} flag as false.
     *
     * @param username the username to disable
     */
    public void disableUser(ByteBuffer username) {
        long stamp = lock.writeLock();
        try {
            short uid = getUidByUsername0(username);
            String hex = ByteBuffers.encodeAsHex(username);
            credentials.put(uid, ENABLED, false);
            tokenManager.deleteAllUserTokens(hex);
        }
        finally {
            lock.unlockWrite(stamp);
        }
    }

    /**
     * Updates {@link #ENABLED} flag for the user as true in credentials table.
     *
     * @param username the username to enable
     */
    public void enableUser(ByteBuffer username) {
        long stamp = lock.writeLock();
        try {
            short uid = getUidByUsername0(username);
            credentials.put(uid, ENABLED, true);
        }
        finally {
            lock.unlockWrite(stamp);
        }
    }

    /**
     * Logout {@code token} so that it is not valid for subsequent access.
     * 
     * @param token
     */
    public void expireAccessToken(AccessToken token) {
        tokenManager.deleteToken(token); // the #tokenManager handles locking
    }

    /**
     * Remove the service {@code token} from the list of those that are valid.
     * <p>
     * <em>This is an alias for the {@link #expireAccessToken(AccessToken)}
     * method.</em>
     * </p>
     * 
     * @param token the service token to remove
     */
    public void expireServiceToken(AccessToken token) {
        expireAccessToken(token);
    }

    /**
     * Login {@code username} for subsequent access with the returned
     * {@link AccessToken}.
     * 
     * @param username
     * @return the AccessToken
     */
    public AccessToken getNewAccessToken(ByteBuffer username) {
        long stamp = lock.tryOptimisticRead();
        checkArgument(isEnabledUsername(username));
        if(!lock.validate(stamp)) {
            lock.readLock();
            try {
                checkArgument(isEnabledUsername0(username));
            }
            finally {
                lock.unlockRead(stamp);
            }
        }
        return tokenManager.addToken(ByteBuffers.encodeAsHex(username)); // tokenManager
                                                                         // handles
                                                                         // locking
    }

    /**
     * Return a new service token.
     * 
     * <p>
     * A service token is an {@link AccessToken} that is not associated with an
     * actual user, but is instead generated based on the
     * {@link #SERVICE_USERNAME} and can be assigned to a non-user service or
     * process.
     * </p>
     * <p>
     * Service tokens do not expire!
     * </p>
     * 
     * @return the new service token
     */
    public AccessToken getNewServiceToken() {
        ByteBuffer bytes = ByteBuffers.fromString(SERVICE_USERNAME);
        return tokenManager.addToken(ByteBuffers.encodeAsHex(bytes));
    }

    /**
     * Return the uid of the user associated with {@code token}.
     * 
     * @param token
     * @return the uid
     */
    public short getUidByAccessToken(AccessToken token) {
        long stamp = lock.tryOptimisticRead();
        ByteBuffer username = getUsernameByAccessToken0(token);
        short uid = getUidByUsername0(username);
        if(!lock.validate(stamp)) {
            username = getUsernameByAccessToken0(token);
            uid = getUidByUsername0(username);
        }
        return uid;
    }

    /**
     * Return the uid of the user identified by {@code username}.
     * 
     * @param username
     * @return the uid
     */
    public short getUidByUsername(ByteBuffer username) {
        long stamp = lock.tryOptimisticRead();
        short uid = getUidByUsername0(username);
        if(!lock.validate(stamp)) {
            stamp = lock.readLock();
            try {
                uid = getUidByUsername0(username);
            }
            finally {
                lock.unlockRead(stamp);
            }
        }
        return uid;
    }

    /**
     * Return the binary format of username associated with {@code token}.
     * 
     * @param token
     * @return the username
     */
    public ByteBuffer getUsernameByAccessToken(AccessToken token) {
        long stamp = lock.tryOptimisticRead();
        ByteBuffer username = getUsernameByAccessToken0(token);
        if(!lock.validate(stamp)) {
            stamp = lock.readLock();
            try {
                username = getUsernameByAccessToken0(token);
            }
            finally {
                lock.unlock(stamp);
            }
        }
        return username;
    }

    /**
     * Return {@code true} if {@code username} exists and is enabled.
     * 
     * @param username the username to check
     * @return {@code true} if {@code username} exists and is enabled
     */
    public boolean isEnabledUsername(ByteBuffer username) {
        long stamp = lock.tryOptimisticRead();
        boolean existing = isExistingUsername0(username);
        boolean enabled = isEnabledUsername0(username);
        if(!lock.validate(stamp)) {
            stamp = lock.readLock();
            try {
                existing = isExistingUsername0(username);
                enabled = isEnabledUsername0(username);
            }
            finally {
                lock.unlockRead(stamp);
            }
        }
        return existing && enabled;
    }

    /**
     * Return {@code true} if {@code username} exists in {@link #backingStore}.
     * 
     * @param username
     * @return {@code true} if {@code username} exists in {@link #backingStore}
     */
    public boolean isExistingUsername(ByteBuffer username) {
        long stamp = lock.tryOptimisticRead();
        boolean valid = isExistingUsername0(username);
        if(!lock.validate(stamp)) {
            stamp = lock.readLock();
            try {
                valid = isExistingUsername0(username);
            }
            finally {
                lock.unlockRead(stamp);
            }
        }
        return valid;
    }

    /**
     * Return {@code true} if {@code username} and {@code password} is a valid
     * combination.
     * 
     * @param username
     * @param password
     * @return {@code true} if {@code username}/{@code password} is valid
     */
    public boolean isExistingUsernamePasswordCombo(ByteBuffer username,
            ByteBuffer password) {
        long stamp = lock.tryOptimisticRead();
        boolean valid = isExistingUsernamePasswordCombo0(username, password);
        if(!lock.validate(stamp)) {
            stamp = lock.readLock();
            try {
                valid = isExistingUsernamePasswordCombo0(username, password);
            }
            finally {
                lock.unlockRead(stamp);
            }
        }
        return valid;
    }

    /**
     * Return {@code true} if {@code token} is a valid AccessToken.
     * 
     * @param token
     * @return {@code true} if {@code token} is valid
     */
    public boolean isValidAccessToken(AccessToken token) {
        return tokenManager.isValidToken(token); // the #tokenManager does
                                                 // locking
    }

    /**
     * Insert credential with {@code username}, {@code password},
     * and {@code salt} into the memory store.
     * 
     * @param username
     * @param password
     * @param salt
     */
    protected void insert(ByteBuffer username, ByteBuffer password,
            ByteBuffer salt) { // visible for
                               // upgrade task
        long stamp = lock.writeLock();
        try {
            boolean enabled = true;
            insert0(username, password, salt, enabled);
        }
        finally {
            lock.unlockWrite(stamp);
        }
    }

    /**
     * Sync any changes made to the memory store to disk.
     */
    private void diskSync() {
        // TODO take a backup in order to make this ACID durable...
        FileChannel channel = FileSystem.getFileChannel(backingStore);
        try {
            channel.position(0);
            Serializables.write(credentials, channel);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        finally {
            FileSystem.closeFileChannel(channel);
        }
    }

    /**
     * Implementation of {@link #getUidByAccessToken(AccessToken)}.
     * 
     * @param username
     * @return the uid
     */
    private short getUidByUsername0(ByteBuffer username) {
        checkArgument(isExistingUsername0(username));
        Map<Short, Object> credsCol = credentials.column(USERNAME_KEY);
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

    /**
     * Implementation of {@link #getUsernameByAccessToken(AccessToken)}.
     * 
     * @param token
     * @return the username
     */
    private ByteBuffer getUsernameByAccessToken0(AccessToken token) {
        String username = tokenManager.getUsernameByAccessToken(token);
        return ByteBuffers.decodeFromHex(username);
    }

    /**
     * Implementation of {@link #insert(ByteBuffer, ByteBuffer, ByteBuffer)}
     * without locking.
     * 
     * @param username
     * @param password
     * @param salt
     */
    private void insert0(ByteBuffer username, ByteBuffer password,
            ByteBuffer salt, boolean enabled) {
        short uid = isExistingUsername0(username) ? getUidByUsername0(username)
                : (short) counter.incrementAndGet();
        credentials.put(uid, USERNAME_KEY, ByteBuffers.encodeAsHex(username));
        credentials.put(uid, PASSWORD_KEY, ByteBuffers.encodeAsHex(password));
        credentials.put(uid, SALT_KEY, ByteBuffers.encodeAsHex(salt));
        credentials.put(uid, ENABLED, enabled);
    }

    /**
     * Implementation of {@link #isExistingUsername(ByteBuffer)}.
     * 
     * @param username
     * @return {@code true} if {@code username} exiasts in {@link #backingStore}
     *         .
     */
    private boolean isEnabledUsername0(ByteBuffer username) {
        short uid = getUidByUsername0(username);
        Object enabled = credentials.get(uid, ENABLED);
        if(enabled == null) {
            enabled = true;
            credentials.put(uid, ENABLED, enabled);
        }
        return (boolean) enabled;
    }

    /**
     * Implementation of {@link #isExistingUsername(ByteBuffer)}.
     * 
     * @param username
     * @return {@code true} if {@code username} exiasts in {@link #backingStore}
     *         .
     */
    private boolean isExistingUsername0(ByteBuffer username) {
        return credentials.containsValue(ByteBuffers.encodeAsHex(username));
    }

    /**
     * Implementation of
     * {@link #isExistingUsernamePasswordCombo(ByteBuffer, ByteBuffer)}.
     * 
     * @param username
     * @param password
     * @return {@code true} if {@code username}/{@code password} is valid
     */
    private boolean isExistingUsernamePasswordCombo0(ByteBuffer username,
            ByteBuffer password) {
        if(isExistingUsername0(username)) {
            short uid = getUidByUsername0(username);
            ByteBuffer salt = ByteBuffers
                    .decodeFromHex((String) credentials.get(uid, SALT_KEY));
            password.rewind();
            password = Passwords.hash(password, salt);
            return ByteBuffers.encodeAsHex(password)
                    .equals((String) credentials.get(uid, PASSWORD_KEY));
        }
        return false;
    }

    /**
     * The {@link AccessTokenManager} handles the work necessary to create,
     * validate and delete AccessTokens for the {@link AccessManager}.
     * 
     * @author Jeff Nelson
     */
    private final static class AccessTokenManager {

        // NOTE: This class does not define #hasCode() or #equals() because the
        // defaults are the desired behaviour.

        /**
         * Return a new {@link AccessTokenManager}.
         * 
         * @param accessTokenTtl
         * @param accessTokenTtlUnit
         * @return the AccessTokenManager
         */
        private static AccessTokenManager create(int accessTokenTtl,
                TimeUnit accessTokenTtlUnit) {
            return new AccessTokenManager(accessTokenTtl, accessTokenTtlUnit);
        }

        private final StampedLock lock = new StampedLock();
        private final SecureRandom srand = new SecureRandom();

        /**
         * The collection of currently valid tokens is maintained as a cache
         * mapping from a raw AccessToken to an AccessTokenWrapper. Each raw
         * AccessToken is unique and "equal" to its corresponding wrapper, which
         * contains metadata about the user and timestamp associated with the
         * access token.
         */
        private final Cache<AccessToken, AccessTokenWrapper> tokens;

        /**
         * Construct a new instance.
         * 
         * @param accessTokenTtl
         * @param accessTokenTtlUnit
         */
        private AccessTokenManager(int accessTokenTtl,
                TimeUnit accessTokenTtlUnit) {
            this.tokens = CacheBuilder.newBuilder()
                    .expireAfterWrite(accessTokenTtl, accessTokenTtlUnit)
                    .build();
        }

        /**
         * Add and return a new access token for {@code username}.
         * 
         * @param username
         * @return the AccessToken
         */
        public AccessToken addToken(String username) {
            long stamp = lock.writeLock();
            try {
                long timestamp = Time.now();
                StringBuilder sb = new StringBuilder();
                sb.append(username);
                sb.append(srand.nextLong());
                sb.append(timestamp);
                AccessToken token = new AccessToken(ByteBuffer.wrap(Hashing
                        .sha256().hashUnencodedChars(sb.toString()).asBytes()));
                AccessTokenWrapper wapper = AccessTokenWrapper.create(token,
                        username, timestamp);
                tokens.put(token, wapper);
                return token;
            }
            finally {
                lock.unlockWrite(stamp);
            }
        }

        /**
         * Invalidate any and all tokens that exist for {@code username}.
         * 
         * @param username
         */
        public void deleteAllUserTokens(String username) {
            long stamp = lock.writeLock();
            try {
                for (AccessToken token : tokens.asMap().keySet()) {
                    if(tokens.getIfPresent(token).getUsername()
                            .equals(username)) {
                        tokens.invalidate(token);
                    }
                }
            }
            finally {
                lock.unlockWrite(stamp);
            }
        }

        /**
         * Invalidate {@code token} if it exists.
         * 
         * @param token
         */
        public void deleteToken(AccessToken token) {
            long stamp = lock.writeLock();
            try {
                tokens.invalidate(token);
            }
            finally {
                lock.unlockWrite(stamp);
            }

        }

        /**
         * Return the username associated with the valid {@code token}.
         * 
         * @param token
         * @return the username if {@code token} is valid
         */
        public String getUsernameByAccessToken(AccessToken token) {
            Preconditions.checkArgument(isValidToken(token),
                    "Access token is no longer invalid.");
            long stamp = lock.tryOptimisticRead();
            String username = tokens.getIfPresent(token).getUsername();
            if(!lock.validate(stamp)) {
                stamp = lock.readLock();
                try {
                    username = tokens.getIfPresent(token).getUsername();
                }
                finally {
                    lock.unlockRead(stamp);
                }
            }
            if(Strings.isNullOrEmpty(username)) {
                throw new IllegalArgumentException(
                        "Access token is no longer valid");
            }
            else {
                return username;
            }
        }

        /**
         * Return {@code true} if {@code token} is valid.
         * 
         * @param token
         * @return {@code true} if {@code token} is valid
         */
        public boolean isValidToken(AccessToken token) {
            long stamp = lock.tryOptimisticRead();
            boolean valid = tokens.getIfPresent(token) != null;
            if(!lock.validate(stamp)) {
                stamp = lock.readLock();
                try {
                    valid = tokens.getIfPresent(token) != null;
                }
                finally {
                    lock.unlockRead(stamp);
                }
            }
            return valid;
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
            uname = uname.equals(SERVICE_USERNAME) ? "BACKGROUND SERVICE"
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

        @Override
        public String toString() {
            return token.toString();
        }
    }

}
