/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse.security;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * A collection of tools for dealing with passwords in a secure manner.
 * 
 * @author Jeff Nelson
 */
public final class Passwords {

    /**
     * The number of bytes used to encode a salt value.
     */
    public static final int SALT_LENGTH = 32;

    /**
     * The number of bytes used to encode a hashed password.
     */
    public static final int PASSWORD_LENGTH = 64;

    /**
     * Return a ByteBuffer that contains a hash of {@code password} with
     * {@code salt}.
     * 
     * @param password
     * @param salt
     * @return the ByteBuffer hash
     */
    public static ByteBuffer hash(ByteBuffer password, ByteBuffer salt) {
        try {
            ByteBuffer unhashed = ByteBuffer.allocate(password.capacity()
                    + salt.capacity());
            unhashed.put(password);
            unhashed.put(salt);
            MessageDigest message = MessageDigest.getInstance("SHA-512");
            ByteBuffer hashed = ByteBuffer
                    .wrap(message.digest(unhashed.array()));
            password = null;
            salt = null;
            Preconditions.checkState(hashed.capacity() == PASSWORD_LENGTH);
            return hashed;
        }
        catch (NoSuchAlgorithmException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Get a random salt value.
     * 
     * @return the salt
     */
    public static ByteBuffer getSalt() {
        SecureRandom srand = new SecureRandom();
        byte[] salt = new byte[SALT_LENGTH];
        srand.nextBytes(salt);
        return ByteBuffer.wrap(salt);
    }

    private Passwords() {}

}
