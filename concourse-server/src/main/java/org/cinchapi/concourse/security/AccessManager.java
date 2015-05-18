/*
 * Copyright (c) 2013-2015 Cinchapi, Inc.
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
package org.cinchapi.concourse.security;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import jsr166e.ConcurrentHashMapV8;
import jsr166e.StampedLock;

import org.cinchapi.concourse.Timestamp;
import org.cinchapi.concourse.annotate.Restricted;
import org.cinchapi.concourse.server.io.FileSystem;
import org.cinchapi.concourse.server.io.Serializables;
import org.cinchapi.concourse.thrift.AccessToken;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.ByteBuffers;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;

import static com.google.common.base.Preconditions.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;

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
        return new AccessManager(backingStore, DEFAULT_SESSION_DURATION);
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
    @VisibleForTesting
    protected static AccessManager createForTesting(String backingStore,
            long accessTokenTtl) {
        return new AccessManager(backingStore, accessTokenTtl);
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
     * The column that contains the list of environments to which a user is
     * explicitly given access in the {@link #credentials} table.
     */
    private static final String ACCESS_KEY = "access";
    /**
     * The default admin password. If the AccessManager does not have any users,
     * it will automatically create an admin with this password.
     */
    private static final ByteBuffer DEFAULT_ADMIN_PASSWORD = ByteBuffers
            .fromString("admin");

    /**
     * The default admin username. If the AccessManager does not have any users,
     * it will automatically create an admin with this username.
     */
    private static final ByteBuffer DEFAULT_ADMIN_USERNAME = ByteBuffers
            .fromString("admin");

    /**
     * The default number of microseconds for which an AccessToken is valid.
     */
    private static final long DEFAULT_SESSION_DURATION = TimeUnit.MICROSECONDS
            .convert(86400, TimeUnit.SECONDS);

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
     * The column that contains a user's {@link UserRole} in the
     * {@link #credentials} table.
     */
    private static final String ROLE_KEY = "role";

    /**
     * The column that contains a user's salt in the {@link #credentials} table.
     */
    private static final String SALT_KEY = "salt";

    /**
     * The store where the credentials are serialized on disk.
     */
    private final String backingStore;

    /**
     * A table in memory that holds the user credentials. Each row in the table
     * is indexed by username.
     */
    private final HashBasedTable<String, String, Object> credentials;

    /**
     * Concurrency control.
     */
    private final StampedLock lock = new StampedLock();

    /**
     * The number of microseconds for which an AccessToken is valid.
     */
    private long sessionDuration;

    /**
     * Information about the user sessions on the server.
     */
    private Map<AccessToken, Session> sessions = new ConcurrentHashMapV8<AccessToken, Session>();

    /**
     * Construct a new instance.
     * 
     * @param backingStore
     * @param accessTokenTtl
     * @param accessTokenTtlUnit
     */
    @SuppressWarnings("unchecked")
    private AccessManager(String backingStore, long accessTokenTtl) {
        this.backingStore = backingStore;
        this.sessionDuration = accessTokenTtl;
        if(FileSystem.getFileSize(backingStore) > 0) {
            ByteBuffer bytes = FileSystem.readBytes(backingStore);
            credentials = Serializables.read(bytes, HashBasedTable.class);
        }
        else {
            credentials = HashBasedTable.create();
            // If there are no credentials (which implies this is a new server)
            // add the default admin username/password
            createUser(DEFAULT_ADMIN_USERNAME, DEFAULT_ADMIN_PASSWORD);
        }
    }

    /**
     * Authenticate the {@code username} and {@code password} and return a
     * {@link AccessToken} if the credentials are valid. Otherwise, return
     * {@code null}.
     * 
     * @param username
     * @param password
     * @return an {@link AccessToken} if the credentials are valid, otherwise
     *         {@code null}.
     */
    @Nullable
    public AccessToken authenticate(ByteBuffer username, ByteBuffer password) {
        long stamp = lock.readLock();
        try {
            String uname = ByteBuffers.encodeAsHex(username);
            if(credentials.containsRow(uname)) {
                ByteBuffer salt = ByteBuffers
                        .decodeFromHex((String) credentials
                                .get(uname, SALT_KEY));
                password.rewind();
                password = Passwords.hash(password, salt);
                if(ByteBuffers.encodeAsHex(password).equals(
                        (String) credentials.get(uname, PASSWORD_KEY))) {
                    ByteBuffer data = ByteBuffers.fromRandomUUID();
                    long expires = Time.now() + sessionDuration;
                    AccessToken token = new AccessToken(data);
                    Session session = new Session(username, expires);
                    sessions.put(token, session);
                    return token;
                }
            }
            return null;
        }
        finally {
            lock.unlockRead(stamp);
        }
    }

    /**
     * Return {@code true} if {@code token} is a valid AccessToken.
     * 
     * @param token
     * @return {@code true} if {@code token} is valid
     */
    public boolean authorize(AccessToken token) {
        Session session = sessions.get(token);
        if(session != null) {
            if(!session.isExpired()) {
                return true;
            }
            else {
                sessions.remove(token);
            }
        }
        return false;
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
        username.rewind();
        password.rewind();
        Preconditions.checkArgument(isAcceptableUsername(username),
                "Username must not be empty, or contain any whitespace.");
        Preconditions.checkArgument(isSecurePassword(password),
                "Password must not be empty, or have fewer than 3 characters.");
        ByteBuffer salt = Passwords.getSalt();
        password = Passwords.hash(password, salt);
        deleteAllUserSessions(username);
        insertUser(username, password, salt);
    }

    /**
     * Logout {@code token} so that it is not valid for subsequent access.
     * 
     * @param token
     */
    public void deauthorize(AccessToken token) {
        sessions.remove(token);
    }

    /**
     * Delete access of the user identified by {@code username}.
     * 
     * @param username
     */
    public void deleteUser(ByteBuffer username) {
        long stamp = lock.writeLock();
        try {
            checkArgument(!username.equals(DEFAULT_ADMIN_USERNAME),
                    "Cannot delete the admin user!");
            String uname = ByteBuffers.encodeAsHex(username);
            credentials.remove(uname, PASSWORD_KEY);
            credentials.remove(uname, SALT_KEY);
            deleteAllUserSessions(username);
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
        List<String> desc = Lists.newArrayList();
        for (Iterator<Session> it = sessions.values().iterator(); it.hasNext();) {
            Session session = it.next();
            if(!session.isExpired()) {
                desc.add(session.toString());
            }
            else {
                it.remove();
            }
        }
        Collections.sort(desc);
        return desc;
    }

    /**
     * Return {@code true} if {@code username} exists in {@link #backingStore}.
     * 
     * @param username
     * @return {@code true} if {@code username} exists in {@link #backingStore}
     */
    public boolean isExistingUsername(ByteBuffer username) {
        long stamp = lock.readLock();
        try {
            return credentials.containsRow(ByteBuffers.encodeAsHex(username));
        }
        finally {
            lock.unlockRead(stamp);
        }
    }

    /**
     * Insert or modify the information in the {@link #credentials} table for
     * the specified access profile.
     * 
     * @param uname
     * @param password
     * @param salt
     */
    protected void insertUser(ByteBuffer username, ByteBuffer password,
            ByteBuffer salt) {
        long stamp = lock.writeLock();
        try {
            String uname = ByteBuffers.encodeAsHex(username);
            credentials.put(uname, PASSWORD_KEY,
                    ByteBuffers.encodeAsHex(password));
            credentials.put(uname, SALT_KEY, ByteBuffers.encodeAsHex(salt));
            diskSync();
        }
        finally {
            lock.unlockWrite(stamp);
        }
    }

    /**
     * Delete all the sessions associated with the {@code username}.
     * 
     * @param username
     */
    private void deleteAllUserSessions(ByteBuffer username) {
        for (Iterator<Entry<AccessToken, Session>> it = sessions.entrySet()
                .iterator(); it.hasNext();) {
            Entry<AccessToken, Session> entry = it.next();
            Session session = entry.getValue();
            if(session.getUsername().equals(username)) {
                it.remove();
            }
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
     * A {@link Session} contains information about a logged in user.
     * 
     * @author Jeff Nelson
     */
    @Immutable
    private final static class Session {

        // NOTE: This class does not implement hashCode or equals because the
        // defualts are the desired behaviour

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
         * The timestamp the session was created.
         */
        private final long created;

        /**
         * The timestamp the session expires.
         */
        private final long expires;

        /**
         * The username associated with the Session.
         */
        private final ByteBuffer username;

        /**
         * Construct a new instance.
         * 
         * @param token
         * @param uname
         * @param expires
         */
        public Session(ByteBuffer username, long expires) {
            this.username = username;
            this.expires = expires;
            this.created = expires - DEFAULT_SESSION_DURATION;
        }

        /**
         * Return the username associated with this Session.
         * 
         * @return the username
         */
        public ByteBuffer getUsername() {
            return ByteBuffers.asReadOnlyBuffer(username);
        }

        /**
         * Return {@code true} if the Session has expired.
         * 
         * @return {@code true} if the session has expired
         */
        public boolean isExpired() {
            return Time.now() > expires;
        }

        @Override
        public String toString() {
            return new StringBuilder()
                    .append(ByteBuffers.getString(getUsername()))
                    .append(" logged in since ")
                    .append(Timestamp.fromMicros(created).getJoda()
                            .toString(DATE_TIME_FORMATTER)).toString();
        }

    }

}
