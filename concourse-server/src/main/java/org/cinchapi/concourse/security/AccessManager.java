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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import org.cinchapi.concourse.annotate.Restricted;
import org.cinchapi.concourse.server.io.FileSystem;
import org.cinchapi.concourse.thrift.AccessToken;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.ByteBuffers;
import org.cinchapi.concourse.util.TStrings;

import static com.google.common.base.Preconditions.*;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
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
     * Return {@code true} if {@code password} is in valid format,
     * in which it must not be null or empty, or contain fewer than
     * 3 characters.
     * 
     * @param password
     * @return {@code true} if {@code password} is valid format
     */
    protected static boolean isSecuredPassword(ByteBuffer password) { // visible
                                                                      // for
                                                                      // testing
        String passwordStr = new String(password.array());
        return !Strings.isNullOrEmpty(passwordStr) && passwordStr.length() >= 3;
    }
    
    // The AccessManager keeps track of user sessions and their associated
    // AccessTokens. An AccessToken is valid for a limited amount of time.
    private static final int ACCESS_TOKEN_TTL = 24;
    private static final TimeUnit ACCESS_TOKEN_TTL_UNIT = TimeUnit.HOURS;
    private final AccessTokenManager tokenManager;
    
    // The AccessManager stores credentials in memory for fast processing and
    // backs them up on disk for persistence.
    private static final String ADMIN_USERNAME = ByteBuffers.encodeAsHex(ByteBuffer
            .wrap("admin".getBytes()));
    private static final String ADMIN_PASSWORD = ByteBuffers.encodeAsHex(ByteBuffer
            .wrap("admin".getBytes()));
    private final Table<Short, String, Object> credentials;
    private final String backingStore;
    
    // Column keys in table of credentials
    private static final String USERNAME_KEY = "username";  // table value as hex
    private static final String PASSWORD_KEY = "password";  // table value as hex    
    private static final String SALT_KEY = "salt";          // table value as hex
    
    // The new uid is generated by incrementing the largest existing uid
    // in the credentials mapping in memory.
    private short maxUid = 0;
    
    // Concurrency controls
    private final ReentrantReadWriteLock master = new ReentrantReadWriteLock();
    private final ReadLock read = master.readLock();
    private final WriteLock write = master.writeLock();
    
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
        this.tokenManager = AccessTokenManager.create(
                accessTokenTtl, accessTokenTtlUnit);
        if (FileSystem.getFileSize(backingStore) > 0) {
            ByteBuffer tableBytes = FileSystem.readBytes(backingStore);
            ByteArrayInputStream bais = new ByteArrayInputStream(
                    ByteBuffers.toByteArray(tableBytes));
            try {
                BufferedInputStream bis = new BufferedInputStream(bais);
                ObjectInput input = new ObjectInputStream(bis);
                credentials = (Table<Short, String, Object>) input.readObject();
                maxUid = Collections.max(credentials.rowKeySet());
            } 
            catch (IOException e) {
                throw Throwables.propagate(e);         
            } 
            catch (ClassNotFoundException e) {
                throw Throwables.propagate(e);
            }
        }
        else {
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
     * <p>If the existing user simply changes the password, the new
     * auto-generated id will not be generated and this username still has
     * the same uid as the time it has been assigned when this
     * {@link AccessManager} is instantiated.</p>
     * 
     * @param username
     * @param password
     */
    public void createUser(ByteBuffer username, ByteBuffer password) {
        Preconditions.checkArgument(isAcceptableUsername(username),
                "Username must not be empty, or contain any whitespace.");
        Preconditions.checkArgument(isSecuredPassword(password),
                "Password must not be empty, or have fewer than 3 characters.");
        write.lock();
        try {
            ByteBuffer salt = Passwords.getSalt();
            password = Passwords.hash(password, salt);
            insert(username, password, salt);
            tokenManager.deleteAllUserTokens(ByteBuffers.encodeAsHex(username));
            diskSync();
        }
        finally {
            write.unlock();
        }
    }
    
    /**
     * Delete access of the user identified by {@code username}.
     * 
     * @param username
     */
    public void deleteUser(ByteBuffer username) {
        write.lock();
        try {
            String hex = ByteBuffers.encodeAsHex(username);
            checkArgument(!hex.equals(ADMIN_USERNAME),
                    "Cannot revoke access for the admin user!");
            short uid = getUidByUsername(username);
            credentials.remove(uid, USERNAME_KEY);
            credentials.remove(uid, PASSWORD_KEY);
            credentials.remove(uid, SALT_KEY);
            tokenManager.deleteAllUserTokens(hex);
            diskSync();
        }
        finally {
            write.unlock();
        }
    }
    
    /**
     * Return the uid of the user associated with {@code token}.
     * 
     * @param token
     * @return the uid
     */
    public short getUidByAccessToken(AccessToken token) {
        ByteBuffer username = getUsernameByAccessToken(token);
        return getUidByUsername(username);
    }
    
    /**
     * Return the uid of the user identified by {@code username}.
     * 
     * @param username
     * @return the uid
     */
    public short getUidByUsername(ByteBuffer username) {
        read.lock();
        try {
            checkArgument(isValidUsername(username));
            Map<Short, Object> credsCol = credentials.column(USERNAME_KEY);
            for (Map.Entry<Short, Object> creds : credsCol.entrySet()) {
                String value = (String) creds.getValue();
                if (value.equals(ByteBuffers.encodeAsHex(username))) {
                    return creds.getKey();
                }
            }
            return -1;  // supress compiler error
                        // but this statement will
                        // never actually execute
        } finally {
            read.unlock();
        }
    }
    
    /**
     * Return the binary format of username associated with {@code token}.
     * 
     * @param token
     * @return the username
     */
    public ByteBuffer getUsernameByAccessToken(AccessToken token) {
        String username = tokenManager.getUsernameByAccessToken(token);
        return ByteBuffers.decodeFromHex(username);
    }
    
    /**
     * Return {@code true} if {@code username} exists in {@link #backingStore}.
     * 
     * @param username
     * @return {@code true} if {@code username} exists in {@link #backingStore}
     */
    public boolean isValidUsername(ByteBuffer username) {      
        return credentials.containsValue(ByteBuffers.encodeAsHex(username));
    }
    
    /**
     * Return {@code true} if {@code username} and {@code password} is a valid
     * combination.
     * 
     * @param username
     * @param password
     * @return {@code true} if {@code username}/{@code password} is valid
     */
    public boolean isValidUserNamePasswordCombo(
            ByteBuffer username, ByteBuffer password) {
        read.lock();
        try {
            if (isValidUsername(username)) {
                short uid = getUidByUsername(username);
                ByteBuffer salt = ByteBuffers.decodeFromHex(
                        (String) credentials.get(uid, SALT_KEY));
                password.rewind();
                password = Passwords.hash(password, salt);
                return ByteBuffers.encodeAsHex(password).equals(
                        (String) credentials.get(uid, PASSWORD_KEY));
            }
            return false;
        }
        finally {
            read.unlock();
        }
    }
    
    /**
     * Login {@code username} for subsequent access with the returned
     * {@link AccessToken}.
     * 
     * @param username
     * @return the AccessToken
     */
    public AccessToken login(ByteBuffer username) {
        read.lock();
        try {
            checkArgument(isValidUsername(username));
            return tokenManager.addToken(ByteBuffers.encodeAsHex(username));
        }
        finally {
            read.unlock();
        }
    }
    
    /**
     * Logout {@code token} so that it is not valid for subsequent access.
     * 
     * @param token
     */
    public void logout(AccessToken token) {
        tokenManager.deleteToken(token); // the #tokenManager handles locking
    }
    
    /**
     * Return {@code true} if {@code token} is a valid AccessToken.
     * 
     * @param token
     * @return {@code true} if {@code token} is valid
     */
    public boolean validate(AccessToken token) {
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
    protected void insert(ByteBuffer username,
            ByteBuffer password, ByteBuffer salt) {         // visible for
                                                            // upgrade task
        short uid = isValidUsername(username) ? 
                getUidByUsername(username) : getNewUid();
        write.lock();
        try {
            credentials.put(uid, USERNAME_KEY, ByteBuffers.encodeAsHex(username));
            credentials.put(uid, PASSWORD_KEY, ByteBuffers.encodeAsHex(password));
            credentials.put(uid, SALT_KEY, ByteBuffers.encodeAsHex(salt));
        }
        finally {
            write.unlock();
        }
    }
    

    
    /**
     * Sync any changes made to the memory store to disk.
     */
    private void diskSync() {
        write.lock();
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutput output = new ObjectOutputStream(
                    new BufferedOutputStream(baos));
            output.writeObject(credentials); 
            output.flush();
            output.close();
            FileChannel channel = FileSystem.getFileChannel(backingStore);
            try {
                channel.write(ByteBuffer.wrap(baos.toByteArray())); 
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
            finally {
                FileSystem.closeFileChannel(channel);
            }
        } 
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        finally {
            write.unlock();
        }
    }
    
    /**
     * Return a unique auto-generated uid.
     * 
     * @return the unique uid
     */
    private short getNewUid() {
        write.lock();
        try {
            this.maxUid += 1;
            return this.maxUid;
        } finally {
            write.unlock();
        }
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

        /**
         * The collection of currently valid tokens is maintained as a cache
         * mapping from a raw AccessToken to an AccessTokenWrapper. Each raw
         * AccessToken is unique and "equal" to its corresponding wrapper, which
         * contains metadata about the user and timestamp associated with the
         * access token.
         */
        private final Cache<AccessToken, AccessTokenWrapper> tokens;
        private final ReentrantReadWriteLock masterLock = new ReentrantReadWriteLock();
        private final ReadLock tokenRead = masterLock.readLock();
        private final WriteLock tokenWrite = masterLock.writeLock();
        private final SecureRandom srand = new SecureRandom();

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
            tokenWrite.lock();
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
                tokenWrite.unlock();
            }
        }

        /**
         * Invalidate any and all tokens that exist for {@code username}.
         * 
         * @param username
         */
        public void deleteAllUserTokens(String username) {
            tokenWrite.lock();
            try {
                for (AccessToken token : tokens.asMap().keySet()) {
                    if(tokens.getIfPresent(token).getUsername()
                            .equals(username)) {
                        tokens.invalidate(token);
                    }
                }
            }
            finally {
                tokenWrite.unlock();
            }
        }

        /**
         * Invalidate {@code token} if it exists.
         * 
         * @param token
         */
        public void deleteToken(AccessToken token) {
            tokenWrite.lock();
            try {
                tokens.invalidate(token);
            }
            finally {
                tokenWrite.unlock();
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
            tokenRead.lock();
            try {
                return tokens.getIfPresent(token).getUsername();
            }
            finally {
                tokenRead.unlock();
            }
        }

        /**
         * Return {@code true} if {@code token} is valid.
         * 
         * @param token
         * @return {@code true} if {@code token} is valid
         */
        public boolean isValidToken(AccessToken token) {
            tokenRead.lock();
            try {
                return tokens.getIfPresent(token) != null;
            }
            finally {
                tokenRead.unlock();
            }
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

        private final AccessToken token;
        private final String username; // hex
        private final long timestamp;

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
