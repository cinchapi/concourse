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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.SecureRandom;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.cinchapi.concourse.annotate.Restricted;
import org.cinchapi.concourse.server.io.Byteable;
import org.cinchapi.concourse.server.io.ByteableCollections;
import org.cinchapi.concourse.server.io.FileSystem;
import org.cinchapi.concourse.thrift.AccessToken;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.ByteBuffers;

import static com.google.common.base.Preconditions.*;

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
    private final Cache<String, AccessToken> tokens;

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

    private final SecureRandom srand = new SecureRandom();

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
        this.tokens = CacheBuilder.newBuilder()
                .expireAfterWrite(accessTokenTtl, accessTokenTtlUnit).build();
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
        checkArgument(isValidUsername(username));
        StringBuilder sb = new StringBuilder();
        String hex = encodeHex(username);
        sb.append(hex);
        sb.append(srand.nextLong());
        sb.append(Time.now());
        AccessToken token = new AccessToken(ByteBuffer.wrap(Hashing.sha256()
                .hashUnencodedChars(sb.toString()).asBytes()));
        tokens.put(hex, token);
        return token;
    }

    /**
     * Deauthorize {@code token} so that it is not valid for subsequent access.
     * 
     * @param token
     */
    public void deauthorize(AccessToken token) {
        write.lock();
        try {
            for (Entry<String, AccessToken> entry : tokens.asMap().entrySet()) {
                if(entry.getValue().equals(token)) {
                    tokens.invalidate(entry.getKey());
                    return;
                }
            }
        }
        finally {
            write.unlock();
        }
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
            tokens.invalidate(encodeHex(username));
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
                tokens.invalidate(encodeHex(username));
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
        read.lock();
        try {
            return tokens.asMap().values().contains(token);
        }
        finally {
            read.unlock();
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
    private boolean isValidUsername(ByteBuffer username) {
        return credentials.get(encodeHex(username)) != null;
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

}
