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
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.cinchapi.concourse.annotate.Restricted;
import org.cinchapi.concourse.server.io.Byteable;
import org.cinchapi.concourse.server.io.ByteableCollections;
import org.cinchapi.concourse.server.io.FileSystem;
import org.cinchapi.concourse.thrift.AccessToken;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.ByteBuffers;

import static com.google.common.base.Preconditions.*;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Maps;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;

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
     * Decode the {@code hex}adeciaml string and return the resulting binary
     * data.
     * 
     * @param hex
     * @return the binary data
     */
    private static ByteBuffer decodeHex(String hex) {
        return ByteBuffer.wrap(BaseEncoding.base16().decode(hex));
    }

    /**
     * Encode the {@code bytes} as a hexadecimal string.
     * 
     * @param bytes
     * @return the hex string
     */
    private static String encodeHex(ByteBuffer bytes) {
        bytes.rewind();
        return BaseEncoding.base16().encode(ByteBuffers.toByteArray(bytes));
    }

    // The AccessManager keeps track of user sessions and their associated
    // AccessTokens. An AccessToken is valid for a limited amount of time.
    private static final int ACCESS_TOKEN_TTL = 24;
    private static final TimeUnit ACCESS_TOKEN_TTL_UNIT = TimeUnit.HOURS;
    private final AccessTokenManager tokenManager;

    // The AccessManager stores credentials in memory for fast processing and
    // backs them up on disk for persistence.
    private static final String ADMIN_USERNAME = encodeHex(ByteBuffer
            .wrap("admin".getBytes()));
    private static final String ADMIN_PASSWORD = encodeHex(ByteBuffer
            .wrap("admin".getBytes()));
    private final Map<String, Credentials> credentials = Maps.newHashMap();
    private final String backingStore;

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
    private AccessManager(String backingStore, int accessTokenTtl,
            TimeUnit accessTokenTtlUnit) {
        this.backingStore = backingStore;
        this.tokenManager = AccessTokenManager.create(accessTokenTtl,
                accessTokenTtlUnit);
        Iterator<ByteBuffer> it = ByteableCollections.iterator(FileSystem
                .readBytes(backingStore));
        while (it.hasNext()) {
            insert(Credentials.fromByteBuffer(it.next()));
        }
        // If there are no credentials (which implies this is a new server) add
        // the default admin username/password
        if(credentials.isEmpty()) {
            grant(decodeHex(ADMIN_USERNAME), decodeHex(ADMIN_PASSWORD));
        }
    }

    /**
     * Authorize {@code username} for subsequent access with the returned
     * {@link AccessToken}.
     * 
     * @param username
     * @return the AccessToken
     */
    public AccessToken authorize(ByteBuffer username) {
        read.lock();
        try {
            checkArgument(isValidUsername(username));
            return tokenManager.addToken(encodeHex(username));
        }
        finally {
            read.unlock();
        }
    }

    /**
     * Deauthorize {@code token} so that it is not valid for subsequent access.
     * 
     * @param token
     */
    public void deauthorize(AccessToken token) {
        tokenManager.deleteToken(token); // the #tokenManager handles locking
    }

    /**
     * Grant access to the user identified by {@code username} with
     * {@code password}.
     * 
     * @param username
     * @param password
     */
    public void grant(ByteBuffer username, ByteBuffer password) {
        write.lock();
        try {
            ByteBuffer salt = Passwords.getSalt();
            password = Passwords.hash(password, salt);
            insert(Credentials.create(encodeHex(username), encodeHex(password),
                    encodeHex(salt)));
            tokenManager.deleteAllUserTokens(encodeHex(username));
            diskSync();
        }
        finally {
            write.unlock();
        }
    }

    /**
     * Revoke access for the user identified by {@code username}.
     * 
     * @param username
     */
    public void revoke(ByteBuffer username) {
        write.lock();
        try {
            String hex = encodeHex(username);
            checkArgument(!hex.equals(ADMIN_USERNAME),
                    "Cannot revoke access for the admin user!");
            if(credentials.remove(hex) != null) {
                tokenManager.deleteAllUserTokens(hex);
                diskSync();
            }
        }
        finally {
            write.unlock();
        }
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
     * Return {@code true} if {@code username} and {@code password} is a valid
     * combination.
     * 
     * @param username
     * @param password
     * @return {@code true} if {@code username}/{@code password} is valid
     */
    public boolean validate(ByteBuffer username, ByteBuffer password) {
        read.lock();
        try {
            Credentials accessRecord = credentials.get(encodeHex(username));
            if(accessRecord != null) {
                ByteBuffer salt = accessRecord.getSalt();
                password.rewind();
                password = Passwords.hash(password, salt);
                return encodeHex(password).equals(accessRecord.getPassword());
            }
            return false;
        }
        finally {
            read.unlock();
        }
    }

    /**
     * Sync any changes made to the memory store to disk.
     */
    private void diskSync() {
        write.lock();
        try {
            FileChannel channel = FileSystem.getFileChannel(backingStore);
            ByteBuffer bytes = ByteableCollections.toByteBuffer(credentials
                    .values());
            try {
                channel.write(bytes);
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
            finally {
                FileSystem.closeFileChannel(channel);
            }
        }
        finally {
            write.unlock();
        }
    }

    /**
     * Insert the {@code creds} into the memory store.
     * 
     * @param creds
     */
    private void insert(Credentials creds) {
        write.lock();
        try {
            credentials.put(creds.getUsername(), creds);
        }
        finally {
            write.unlock();
        }
    }

    /**
     * Return {@code true} if {@code username} is valid.
     * 
     * @param username
     * @return {@code true} if {@code username} is valid
     */
    public boolean isValidUsername(ByteBuffer username) {
        return credentials.get(encodeHex(username)) != null;
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
            masterLock.writeLock().lock();
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
                masterLock.writeLock().unlock();
            }
        }

        /**
         * Invalidate any and all tokens that exist for {@code username}.
         * 
         * @param username
         */
        public void deleteAllUserTokens(String username) {
            masterLock.writeLock().lock();
            try {
                for (AccessToken token : tokens.asMap().keySet()) {
                    if(tokens.getIfPresent(token).getUsername()
                            .equals(username)) {
                        tokens.invalidate(token);
                    }
                }
            }
            finally {
                masterLock.writeLock().unlock();
            }
        }

        /**
         * Invalidate {@code token} if it exists.
         * 
         * @param token
         */
        public void deleteToken(AccessToken token) {
            masterLock.writeLock().lock();
            try {
                tokens.invalidate(token);
            }
            finally {
                masterLock.writeLock().unlock();
            }

        }

        /**
         * Return {@code true} if {@code token} is valid.
         * 
         * @param token
         * @return {@code true} if {@code token} is valid
         */
        public boolean isValidToken(AccessToken token) {
            masterLock.readLock().lock();
            try {
                return tokens.getIfPresent(token) != null;
            }
            finally {
                masterLock.readLock().unlock();
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

    /**
     * A grouping of a username, password and salt that together identify a
     * valid authentication scheme for a user.
     * 
     * @author jnelson
     */
    private static final class Credentials implements Byteable {

        /**
         * Create a new set of Credentials for {@code username} and
         * {@code password} hashed with {@code salt}.
         * 
         * @param username
         * @param password
         * @param salt
         * @return the Credentials
         */
        public static Credentials create(String username, String password,
                String salt) {
            return new Credentials(username, password, salt);
        }

        /**
         * Deserialize the Credentials that are encoded in {@code bytes}.
         * 
         * @param bytes
         * @return the Credentials
         */
        public static Credentials fromByteBuffer(ByteBuffer bytes) {
            String password = encodeHex(ByteBuffers.get(bytes,
                    Passwords.PASSWORD_LENGTH));
            String salt = encodeHex(ByteBuffers.get(bytes,
                    Passwords.SALT_LENGTH));
            String username = encodeHex(ByteBuffers.get(bytes,
                    bytes.remaining()));
            return new Credentials(username, password, salt);
        }

        // These are hex encoded values. It Is okay to keep them in memory as a
        // strings since the actual password can't be reconstructed from the
        // string hash.
        private final String username;
        private final String password;
        private final String salt;

        /**
         * Construct a new instance.
         * 
         * @param username
         * @param password
         * @param salt
         */
        private Credentials(String username, String password, String salt) {
            this.username = username;
            this.password = password;
            this.salt = salt;
        }

        @Override
        public ByteBuffer getBytes() {
            ByteBuffer bytes = ByteBuffer.allocate(size());
            bytes.put(decodeHex(password));
            bytes.put(decodeHex(salt));
            bytes.put(decodeHex(username));
            bytes.rewind();
            return ByteBuffers.asReadOnlyBuffer(bytes);
        }

        /**
         * Return the hex encoded password.
         * 
         * @return the password hex
         */
        public String getPassword() {
            return password;
        }

        /**
         * Return the salt as a ByteBuffer.
         * 
         * @return the salt bytes
         */
        public ByteBuffer getSalt() {
            return decodeHex(salt);
        }

        /**
         * Return the hex encoded username.
         * 
         * @return the username hex
         */
        public String getUsername() {
            return username;
        }

        @Override
        public int size() {
            return Passwords.PASSWORD_LENGTH + Passwords.SALT_LENGTH
                    + decodeHex(username).capacity();
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("username: " + username).append(
                    System.getProperty("line.seperator"));
            sb.append("password: " + password).append(
                    System.getProperty("line.seperator"));
            sb.append("salt: " + salt);
            return sb.toString();
        }

    }
    
    /**
     * The validator to validate parsed username from JCommander.
     * 
     * @author knd
     */
    public static class UsernameValidator implements IParameterValidator {
        
        public String validationErrorMsg = "Username cannot be empty " +
                "or contain whitespaces.";
        
        @Override
        public void validate(String name, String value) throws ParameterException { 
            if(Strings.isNullOrEmpty(value)) {
                throw new ParameterException(validationErrorMsg);
            }
            Matcher matcher = Pattern.compile("\\s").matcher(value);
            boolean hasWhiteSpace = matcher.find();
            if (hasWhiteSpace) {
                throw new ParameterException(validationErrorMsg);
            }
        }
        
        /**
         * Checks if the username is valid.
         * 
         * @param value
         * @return true/false
         */
        public boolean isValidUsername(String value) {
            try {
                validate(null, value);
                return true;
            }
            catch (Exception e) {
                return false;
            }
        }
        
    }
    
    /**
     * The validator to validate parsed password from JCommander.
     * 
     * @author knd
     */
    public static class PasswordValidator implements IParameterValidator {
        
        public String validationErrorMsg = "Password " +
                "cannot be empty, or have fewer than 3 characters, " +
                "or contain whitespaces.";
        
        @Override
        public void validate(String name, String value) throws ParameterException {
            if(Strings.isNullOrEmpty(value)) {
                throw new ParameterException(validationErrorMsg);
            }
            else if (value.length() < 3) {
                throw new ParameterException(validationErrorMsg);
            }
            Matcher matcher = Pattern.compile("\\s").matcher(value);
            boolean hasWhiteSpace = matcher.find();
            if (hasWhiteSpace) {
                throw new ParameterException(validationErrorMsg);
            }
        }
        
        /**
         * Check if the password is valid.
         * 
         * @param value
         * @return true/false
         */
        public boolean isValidPassword(String value) {
            try {
                validate(null, value);
                return true;
            }
            catch (Exception e) {
                return false;
            }
        }
        
    }

}
