/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2015 Jeff Nelson, Cinchapi Software Collective
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

import org.cinchapi.concourse.annotate.Restricted;
import org.cinchapi.concourse.server.io.Byteable;
import org.cinchapi.concourse.server.io.ByteableCollections;
import org.cinchapi.concourse.server.io.FileSystem;
import org.cinchapi.concourse.util.ByteBuffers;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;

/**
 * The {@link LegacyAccessManager} controls access to the pre-0.5.0
 * Concourse server by keeping tracking of valid credentials and
 * handling authentication requests. This LegacyAccessManager is used
 * to upgrade pre-0.5.0 user credentials to work with {@link AccessManager}.
 * 
 * @author knd
 */
public class LegacyAccessManager {

    /**
     * Create a LegacyAccessManager that stores its legacy
     * credentials in {@code backingStore}.
     * 
     * @param backingStore
     * @return the LegacyAccessManager
     */
    public static LegacyAccessManager create(String backingStore) {
        return new LegacyAccessManager(backingStore);
    }

    // The legacy credentials are stored in memory
    private final Map<String, Credentials> credentials = Maps
            .newLinkedHashMap();

    /**
     * Construct a new instance.
     * 
     * @param backingStore
     */
    private LegacyAccessManager(String backingStore) {
        Iterator<ByteBuffer> it = ByteableCollections.iterator(FileSystem
                .readBytes(backingStore));
        while (it.hasNext()) {
            LegacyAccessManager.Credentials creds = Credentials
                    .fromByteBuffer(it.next());
            credentials.put(creds.getUsername(), creds);
        }
    }

    /**
     * Transfer the legacy {@link #credentials} managed by
     * this LegacyAccessManager to the specified {@code AccessManager} so that
     * they are now working with and managed by {@code AccessManager}.
     * 
     * @param manager
     */
    public void transferCredentials(AccessManager manager) {
        for (LegacyAccessManager.Credentials creds : credentials.values()) {
            manager.insert(ByteBuffers.decodeFromHex(creds.getUsername()),
                    ByteBuffers.decodeFromHex(creds.getPassword()),
                    creds.getSalt());
        }
    }

    /**
     * Create a user with {@code username} and {@code password} as
     * a legacy {@link Credentials} managed by this LegacyAccessManager.
     * This method should only be used for testing.
     * 
     * @param username
     * @param password
     */
    @Restricted
    protected void createUser(ByteBuffer username, ByteBuffer password) { // visible
                                                                          // for
                                                                          // testing
        ByteBuffer salt = Passwords.getSalt();
        password = Passwords.hash(password, salt);
        Credentials creds = LegacyAccessManager.Credentials.create(
                ByteBuffers.encodeAsHex(username),
                ByteBuffers.encodeAsHex(password),
                ByteBuffers.encodeAsHex(salt));
        credentials.put(creds.getUsername(), creds);
    }

    /**
     * Sync the memory store {@link #credentials} to disk
     * at {@code backingStore}. This method should only be used
     * for testing.
     * 
     * @param backingStore
     */
    @Restricted
    protected void diskSync(String backingStore) { // visible
                                                   // for
                                                   // testing
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

    /**
     * A grouping of a username, password and salt that together identify a
     * valid authentication scheme for a user.
     * 
     * @author knd
     */
    private static final class Credentials implements Byteable {

        /**
         * Create a new set of Credentials for {@code username},
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
            String password = ByteBuffers.encodeAsHex(ByteBuffers.get(bytes,
                    Passwords.PASSWORD_LENGTH));
            String salt = ByteBuffers.encodeAsHex(ByteBuffers.get(bytes,
                    Passwords.SALT_LENGTH));
            String username = ByteBuffers.encodeAsHex(ByteBuffers.get(bytes,
                    bytes.remaining()));
            return new Credentials(username, password, salt);
        }

        private final String password;
        private final String salt;
        // These are hex encoded values. It Is okay to keep them in memory as a
        // strings since the actual password can't be reconstructed from the
        // string hash.
        private final String username;

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
            copyTo(bytes);
            bytes.rewind();
            return bytes;
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
            return ByteBuffers.decodeFromHex(salt);
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
                    + ByteBuffers.decodeFromHex(username).capacity();
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("username: " + username).append(
                    System.getProperty("line.separator"));
            sb.append("password: " + password).append(
                    System.getProperty("line.separator"));
            sb.append("salt: " + salt).append(
                    System.getProperty("line.separator"));
            return sb.toString();
        }

        @Override
        public void copyTo(ByteBuffer buffer) {
            buffer.put(ByteBuffers.decodeFromHex(password));
            buffer.put(ByteBuffers.decodeFromHex(salt));
            buffer.put(ByteBuffers.decodeFromHex(username));
        }

    }

}
