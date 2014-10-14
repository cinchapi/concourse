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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.cinchapi.concourse.annotate.Restricted;
import org.cinchapi.concourse.server.io.FileSystem;
import org.cinchapi.concourse.server.io.Serializables;
import org.cinchapi.concourse.thrift.AccessToken;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.ByteBuffers;
import org.cinchapi.concourse.util.TStrings;
import org.cinchapi.vendor.jsr166e.StampedLock;

import static com.google.common.base.Preconditions.*;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.HashBasedTable;
import com.google.common.hash.Hashing;

/**
 * The {@link AccessManager} controls access to the server by keeping tracking
 * of valid credentials and handling authentication requests.
 * 
 * @author jnelson
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
    protected static boolean isAcceptableUsername(ByteBuffer username) { // visible
                                                                         // for
                                                                         // testing
        String usernameStr = new String(username.array());
        return !Strings.isNullOrEmpty(usernameStr)
                && !usernameStr
                        .contains(TStrings.REGEX_GROUP_OF_ONE_OR_MORE_WHITESPACE_CHARS);
    }

    /**
     * Return {@code true} if {@code username} is reserved.
     * 
     * @param username
     * @return {@code true} if ({@code username} is reserved
     */
    protected static boolean isReservedUsername(ByteBuffer username) { // visible
                                                                       // for
                                                                       // testing
        String usernameStr = new String(username.array());
        return usernameStr.equalsIgnoreCase(DELETED_USERNAME);
    }

    /**
     * Return {@code true} if {@code password} is in valid format,
     * in which it must not be null or empty, or contain fewer than
     * 3 characters.
     * 
     * @param password
     * @return {@code true} if {@code password} is valid format
     */
    protected static boolean isSecurePassword(ByteBuffer password) { // visible
                                                                     // for
                                                                     // testing
        String passwordStr = new String(password.array());
        return !Strings.isNullOrEmpty(passwordStr) && passwordStr.length() >= 3;
    }

    // The AccessManager keeps track of user sessions and their associated
    // AccessTokens. An AccessToken is valid for a limited amount of time.
    private static final int ACCESS_TOKEN_TTL = 24;
    private static final TimeUnit ACCESS_TOKEN_TTL_UNIT = TimeUnit.HOURS;
    private static final String ADMIN_PASSWORD = ByteBuffers
            .encodeAsHex(ByteBuffer.wrap("admin".getBytes()));

    // The AccessManager stores credentials in memory for fast processing and
    // backs them up on disk for persistence.
    private static final String ADMIN_USERNAME = ByteBuffers
            .encodeAsHex(ByteBuffer.wrap("admin".getBytes()));
    // Reserved username that is used to display deleted user
    // in history output.
    private static final String DELETED_USERNAME = "Unknown";
    private static final String PASSWORD_KEY = "password"; // table value as hex
    private static final String SALT_KEY = "salt"; // table value as hex

    // Column keys in table of credentials.
    private static final String USERNAME_KEY = "username"; // table value as hex

    private final String backingStore;
    // The new uid is generated by incrementing the largest existing uid
    // in the credentials mapping in memory.
    private AtomicInteger counter;

    private final HashBasedTable<Short, String, Object> credentials;

    // Concurrency controls.
    private final StampedLock lock = new StampedLock();

    // Deals with all the access tokens for user sessions
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
                    Collections.max(credentials.rowKeySet()));
        }
        else {
            counter = new AtomicInteger(0);
            credentials = HashBasedTable.create();
            // If there are no credentials (which implies this is a new server)
            // add the default admin username/password
            createUser(ByteBuffers.decodeFromHex(ADMIN_USERNAME),
                    ByteBuffers.decodeFromHex(ADMIN_PASSWORD));
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
        Preconditions.checkArgument(!isReservedUsername(username),
                "This username is reserved.");
        Preconditions.checkArgument(isSecurePassword(password),
                "Password must not be empty, or have fewer than 3 characters.");
        long stamp = lock.writeLock();
        try {
            ByteBuffer salt = Passwords.getSalt();
            password = Passwords.hash(password, salt);
            insert0(username, password, salt);
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
            checkArgument(!hex.equals(ADMIN_USERNAME),
                    "Cannot revoke access for the admin user!");
            short uid = getUidByUsername0(username);
            credentials.remove(uid, USERNAME_KEY);
            credentials.remove(uid, PASSWORD_KEY);
            credentials.remove(uid, SALT_KEY);
            tokenManager.deleteAllUserTokens(hex);
            diskSync();
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
     * Login {@code username} for subsequent access with the returned
     * {@link AccessToken}.
     * 
     * @param username
     * @return the AccessToken
     */
    public AccessToken getNewAccessToken(ByteBuffer username) {
        long stamp = lock.tryOptimisticRead();
        checkArgument(isExistingUsername0(username));
        if(!lock.validate(stamp)) {
            lock.readLock();
            try {
                checkArgument(isExistingUsername0(username));
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
            insert0(username, password, salt);
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
        return -1; // supress compiler error
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
            ByteBuffer salt) {
        short uid = isExistingUsername0(username) ? getUidByUsername0(username)
                : (short) counter.incrementAndGet();
        credentials.put(uid, USERNAME_KEY, ByteBuffers.encodeAsHex(username));
        credentials.put(uid, PASSWORD_KEY, ByteBuffers.encodeAsHex(password));
        credentials.put(uid, SALT_KEY, ByteBuffers.encodeAsHex(salt));
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
            ByteBuffer salt = ByteBuffers.decodeFromHex((String) credentials
                    .get(uid, SALT_KEY));
            password.rewind();
            password = Passwords.hash(password, salt);
            return ByteBuffers.encodeAsHex(password).equals(
                    (String) credentials.get(uid, PASSWORD_KEY));
        }
        return false;
    }

    /**
     * The {@link AccessTokenManager} handles the work necessary to create,
     * validate and delete AccessTokens for the {@link AccessManager}.
     * 
     * @author jnelson
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
     * @author jnelson
     */
    private static class AccessTokenWrapper {

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
