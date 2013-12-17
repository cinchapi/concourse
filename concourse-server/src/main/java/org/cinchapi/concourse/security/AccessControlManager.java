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
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import org.cinchapi.concourse.server.io.Byteable;
import org.cinchapi.concourse.server.io.ByteableCollections;
import org.cinchapi.concourse.server.io.FileSystem;
import org.cinchapi.concourse.util.ByteBuffers;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;

/**
 * The AccessControlManager coordinates access to the server.
 * 
 * @author jnelson
 */
public class AccessControlManager {

    private static final ByteBuffer ADMIN_USERNAME = ByteBuffer.wrap("admin"
            .getBytes());
    private static final ByteBuffer ADMIN_PASSWORD = ADMIN_USERNAME;

    /**
     * The file where the credentials are stored on disk.
     */
    private static final String CREDENTIAL_STORE = ".credentials";

    /**
     * A mapping from username to credential.
     */
    private static final Map<ByteBuffer, Credential> CREDENTIALS;
    static {
        CREDENTIALS = Maps.newHashMap();
        Iterator<ByteBuffer> it = ByteableCollections.iterator(FileSystem
                .readBytes(CREDENTIAL_STORE));
        while (it.hasNext()) {
            insert(Credential.fromByteBuffer(it.next()));
        }

        // If there are no credential (which implies this is a new server) add
        // the default admin username/password
        if(CREDENTIALS.isEmpty()) {
            grantAccess(ADMIN_USERNAME, ADMIN_PASSWORD);
        }
    }

    // private static final Map<AccessToken, ByteBuffer> SESSIONS = Maps
    // .newHashMap();

    /**
     * Return true if {@code username} and {@code password} is a valid
     * combination.
     * 
     * @param username
     * @param password
     * @return {@code true} if the username/password combination is valid
     */
    public synchronized static boolean isValidUsernameAndPassword(
            ByteBuffer username, ByteBuffer password) {
        if(CREDENTIALS.containsKey(username)) {
            Credential credential = CREDENTIALS.get(username);
            return credential.getPassword().equals(
                    Passwords.hash(password, credential.getSalt()));
        }
        return false;
    }

    /**
     * Insert {@code credential} into the map alongside the other credentials.
     * 
     * @param credential
     */
    private synchronized static void insert(Credential credential) {
        CREDENTIALS.put(credential.getUsername(), credential);
    }

    /**
     * Grant access to a user identified by {@code username} and
     * {@code password}.
     * 
     * @param username
     * @param password
     */
    public synchronized static void grantAccess(ByteBuffer username,
            ByteBuffer password) {
        ByteBuffer salt = Passwords.getSalt();
        password = Passwords.hash(password, salt);
        insert(Credential.create(username, password, salt));
    }

    /**
     * Sync the credentials held in memory to disk.
     */
    private static void diskSyncCredentials() {
        FileChannel channel = FileSystem.getFileChannel(CREDENTIAL_STORE);
        ByteBuffer bytes = ByteableCollections.toByteBuffer(CREDENTIALS
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

    /**
     * Revoke access for the user identified by {@code username} and
     * {@code password}.
     * 
     * @param username
     */
    public synchronized static void revokeAccess(ByteBuffer username) {
        Preconditions.checkArgument(!username.equals(ADMIN_USERNAME),
                "Cannot revoke access for the admin user");
        CREDENTIALS.remove(username);
        diskSyncCredentials();
    }

    /**
     * A record that associates a username, password and salt. Credential
     * equality and hash code are defined strictly in terms of the username.
     * 
     * @author jnelson
     */
    @Immutable
    @ThreadSafe
    private static final class Credential implements Byteable {

        /**
         * Create a new Credential for {@code username}, {@code password} and
         * {@code salt}.
         * 
         * @param username
         * @param password
         * @param salt
         * @return the Credential
         */
        public static Credential create(ByteBuffer username,
                ByteBuffer password, ByteBuffer salt) {
            return new Credential(username, password, salt);
        }

        /**
         * Return the Credential that is encoded in {@code bytes}.
         * 
         * @param bytes
         * @return the Credential
         */
        public static Credential fromByteBuffer(ByteBuffer bytes) {
            ByteBuffer password = ByteBuffers.get(bytes,
                    Passwords.PASSWORD_LENGTH);
            ByteBuffer salt = ByteBuffers.get(bytes, Passwords.SALT_LENGTH);
            ByteBuffer username = ByteBuffers.get(bytes, bytes.remaining());
            return new Credential(username, password, salt);
        }

        private final ByteBuffer username;
        private final ByteBuffer password;
        private final ByteBuffer salt;

        /**
         * Construct a new instance.
         * 
         * @param username
         * @param password
         * @param salt
         */
        private Credential(ByteBuffer username, ByteBuffer password,
                ByteBuffer salt) {
            Preconditions
                    .checkArgument(password.capacity() == Passwords.PASSWORD_LENGTH
                            && salt.capacity() == Passwords.SALT_LENGTH);
            this.username = username;
            this.password = password;
            this.salt = salt;
        }

        /**
         * Construct a new instance.
         * 
         * @param username
         */
        private Credential(ByteBuffer username) {
            this.username = username;
            this.password = null;
            this.salt = null;
        }

        @Override
        public int hashCode() {
            return Objects.hash(username);
        }

        @Override
        public boolean equals(Object obj) {
            if(obj instanceof Credential) {
                return username.equals(((Credential) obj).username);
            }
            return false;
        }

        /**
         * Return the salt associated with this credential.
         * 
         * @return the salt
         */
        public ByteBuffer getSalt() {
            return ByteBuffers.asReadOnlyBuffer(salt);
        }

        /**
         * Return the username associated with this credential.
         * 
         * @return the username
         */
        public ByteBuffer getUsername() {
            return ByteBuffers.asReadOnlyBuffer(username);
        }

        /**
         * Return the password associated with this credential.
         * 
         * @return the password
         */
        public ByteBuffer getPassword() {
            return ByteBuffers.asReadOnlyBuffer(password);
        }

        @Override
        public int size() {
            return Passwords.PASSWORD_LENGTH + Passwords.SALT_LENGTH
                    + username.capacity();
        }

        @Override
        public ByteBuffer getBytes() {
            ByteBuffer bytes = ByteBuffer.allocate(size());
            bytes.put(password);
            bytes.put(salt);
            bytes.put(username);
            bytes.rewind();
            return ByteBuffers.asReadOnlyBuffer(bytes);
        }

    }
}
