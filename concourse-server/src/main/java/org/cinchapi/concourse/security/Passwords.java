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

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * A collection of tools for dealing with passwords in a secure manner.
 * 
 * @author jnelson
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
