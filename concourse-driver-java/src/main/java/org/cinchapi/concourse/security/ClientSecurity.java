/*
 * Copyright (c) 2013-2015 Cinchapi, Inc.
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
 * @author Jeff Nelson
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
