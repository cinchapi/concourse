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
import java.security.SecureRandom;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import com.google.common.base.Throwables;

/**
 * A collection of tools to deal with security on the client. <strong>These
 * functions are not suitable for server side security!!!</strong>
 * 
 * @author jnelson
 */
public class ClientSecurity {

    /**
     * The client specific key that is used to encrypt/decrypt data. This key is
     * valid for the duration of the client JVM's lifetime.
     */
    private static final byte[] KEY = new byte[16];
    static {
        new SecureRandom().nextBytes(KEY);
    }

    /**
     * Encrypt the specified {@code data} string.
     * 
     * @param data
     * @return a byte buffer with the encrypted data
     */
    public static ByteBuffer encrypt(String data) {
        try {
            Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
            SecretKeySpec secretKey = new SecretKeySpec(KEY, "AES");
            cipher.init(Cipher.ENCRYPT_MODE, secretKey);
            return ByteBuffer.wrap(cipher.doFinal(data.getBytes()));
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }

    }

    /**
     * Decrpyt the specified {@code data} buffer. This method returns a byte
     * buffer instead of a string so that the caller can set the buffer to
     * {@code null} once it is not longer in useful scope. Doing so will help to
     * narrow the window in which vulnerable information resides in memory.
     * 
     * @param data
     * @return a byte buffer with the encrypted data.
     */
    public static ByteBuffer decrypt(ByteBuffer data) {
        try {
            Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
            SecretKeySpec secretKey = new SecretKeySpec(KEY, "AES");
            cipher.init(Cipher.DECRYPT_MODE, secretKey);
            return ByteBuffer.wrap(cipher.doFinal(data.array()));
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

}
