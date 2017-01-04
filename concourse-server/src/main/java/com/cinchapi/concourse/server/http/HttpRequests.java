/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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
package com.cinchapi.concourse.server.http;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;

import com.cinchapi.concourse.security.ClientSecurity;
import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.util.ByteBuffers;
import com.google.common.base.Strings;
import com.google.common.hash.Hashing;
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
     * @param request
     * @return an array with three elements: the first contains the actual
     *         {@link AccessToken} and the second contains the environment that
     *         the token was encoded with and the third contains the fingerprint
     * @throws GeneralSecurityException
     */
    public static HttpAuthToken decodeAuthToken(String token)
            throws GeneralSecurityException {
        ByteBuffer cryptPack = ByteBuffer.wrap(BaseEncoding.base64Url().decode(
                token));
        String pack = ByteBuffers.getString(ClientSecurity.decrypt(cryptPack));
        String[] toks = pack.split("\\|");
        return new HttpAuthToken(new AccessToken(ByteBuffer.wrap(BaseEncoding
                .base32Hex().decode(toks[0]))), toks[1], toks[2]);

    }

    /**
     * Encode an auth token. The encoded token embeds information about the
     * {@code environment} so that we can perform sanity checks that ensure the
     * environment specified in the URL is one that the auth token was actually
     * designed to access.
     * 
     * @param token
     * @param environment
     * @param request
     * @return the encoded auth token
     */
    public static String encodeAuthToken(AccessToken token, String environment,
            HttpRequest request) {
        String base32Token = BaseEncoding.base32Hex().encode(token.getData());
        String fingerprint = getFingerprint(request);
        String pack = base32Token + "|" + environment + "|" + fingerprint;
        ByteBuffer cryptPack = ClientSecurity.encrypt(pack);
        String base64CryptPack = BaseEncoding.base64Url().encode(
                ByteBuffers.toByteArray(cryptPack));
        return base64CryptPack;
    }

    /**
     * Return an MD5 hash that represents the fingerprint (ip address and
     * browser information) for the client.
     * 
     * @param request
     * @return the client fingerprint
     */
    public static String getFingerprint(HttpRequest request) {
        StringBuilder sb = new StringBuilder();
        sb.append(getUserAgent(request));
        sb.append(getIpAddress(request));
        return Hashing.md5().hashUnencodedChars(sb).toString();
    }

    /**
     * Return the client IP address, making a best effort to determine the
     * original IP if the request has been proxied.
     * 
     * @param request - the client's request
     * @return the client IP address
     */
    public static String getIpAddress(HttpRequest request) {
        String ip = request.ip();
        try {
            InetAddress address = InetAddress.getByName(ip);
            if(address.isAnyLocalAddress() || address.isLoopbackAddress()) {
                String forwarded = request.headers("X-Forwarded-For");
                ip = !Strings.isNullOrEmpty(forwarded) ? forwarded : ip;
            }
        }
        catch (Exception e) {/* noop */}
        return ip;
    }

    /**
     * Return the client user-agent, making a best effort to check any relevant
     * headers for the information.
     * 
     * @param request - the client's request
     * @return the client user agent
     */
    public static String getUserAgent(HttpRequest request) {
        String userAgent = request.headers("User-Agent");
        if(Strings.isNullOrEmpty(userAgent)) {
            userAgent = "idk";
        }
        return userAgent;
    }
}
