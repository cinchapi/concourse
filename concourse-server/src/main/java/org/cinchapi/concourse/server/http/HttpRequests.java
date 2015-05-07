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
package org.cinchapi.concourse.server.http;

import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;

import org.cinchapi.concourse.security.ClientSecurity;
import org.cinchapi.concourse.thrift.AccessToken;
import org.cinchapi.concourse.util.ByteBuffers;
import com.google.common.io.BaseEncoding;

/**
 * A collection of utility methods for dealing with HTTP requests.
 * 
 * @author Jeff Nelson
 */
public class HttpRequests {

    /**
     * Decode an auth token.
     * 
     * @param token
     * @return an array with two elements: the first contains the actual
     *         {@link AccessToken} and the second contains the environment that
     *         the token was encoded with
     * @throws GeneralSecurityException
     */
    public static Object[] decodeAuthToken(String token)
            throws GeneralSecurityException {
        ByteBuffer cryptPack = ByteBuffer.wrap(BaseEncoding.base64Url().decode(
                token));
        String pack = ByteBuffers.getString(ClientSecurity.decrypt(cryptPack));
        String[] toks = pack.split("\\|");
        Object[] parts = new Object[2];
        parts[0] = new AccessToken(ByteBuffer.wrap(BaseEncoding.base32Hex()
                .decode(toks[0])));
        parts[1] = toks[1];
        return parts;

    }

    /**
     * Encode an auth token. The encoded token embeds information about the
     * {@code environment} so that we can perform sanity checks that ensure the
     * environment specified in the URL is one that the auth token was actually
     * designed to access.
     * 
     * @param token
     * @param environment
     * @return the encoded auth token
     */
    public static String encodeAuthToken(AccessToken token, String environment) {
        String base32Token = BaseEncoding.base32Hex().encode(token.getData());
        String pack = base32Token + "|" + environment;
        ByteBuffer cryptPack = ClientSecurity.encrypt(pack);
        String base64CryptPack = BaseEncoding.base64Url().encode(
                ByteBuffers.toByteArray(cryptPack));
        return base64CryptPack;
    }
}
