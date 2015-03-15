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

import javax.annotation.Nullable;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;

import org.cinchapi.concourse.security.ClientSecurity;
import org.cinchapi.concourse.server.GlobalState;
import org.cinchapi.concourse.thrift.AccessToken;
import org.cinchapi.concourse.util.ByteBuffers;
import org.cinchapi.concourse.util.Reflection;
import org.eclipse.jetty.http.HttpURI;

import com.google.common.base.Throwables;
import com.google.common.io.BaseEncoding;

/**
 * A collection of utility methods for dealing with HTTP requests.
 * 
 * @author Jeff Nelson
 */
public class HttpRequests {

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
     * Search through the {@code request} for the value of the {@code name}
     * cookie, if it exists.
     * 
     * @param name
     * @param request
     * @return the cookie value or {@code null}
     */
    @Nullable
    public static String findCookieValue(String name, HttpServletRequest request) {
        if(request.getCookies() != null) {
            for (Cookie cookie : request.getCookies()) {
                if(cookie.getName().equals(name)) {
                    return cookie.getValue();
                }
            }
        }
        return null;
    }

    /**
     * Given the {@code target} of the request, check to see if the user has
     * specified an environment that matches the environment embedded in the
     * access token cookie. If so, strip the environment from the target
     * 
     * @param target
     * @param baseRequest
     * @param request
     */
    public static void rewrite(String target,
            org.eclipse.jetty.server.Request baseRequest,
            HttpServletRequest request) {
        String[] targetParts = target.split("/");
        boolean rewrite = false;
        if(targetParts.length >= 2) {
            String targetEnv = targetParts[1];
            if(targetEnv.equals("login")) {
                // Do not rewrite login with no declared environment. Just set
                // the request attribute to use the DEFAULT ENVIRONMENT
                targetEnv = GlobalState.DEFAULT_ENVIRONMENT;
            }
            else if(targetParts.length >= 3 && targetParts[2].equals("login")) {
                // Rewrite login with declared environment like we would all
                // other requests, but tell the request attribute to use the
                // declared environment instead of the one in the cookie.
                target = target.replaceFirst(targetEnv, "").replaceAll("//",
                        "/");
                rewrite = true;
            }
            else {
                // Rewrite all requests to drop the declared environment from
                // the path and use the request attributes to specify meta
                // information
                String token = findCookieValue(
                        GlobalState.HTTP_AUTH_TOKEN_COOKIE, request);
                if(token != null) {
                    try {
                        Object[] auth = decodeAuthToken(token);
                        AccessToken access = (AccessToken) auth[0];
                        String authEnv = (String) auth[1];
                        if(authEnv.equals(targetEnv)) {
                            target = target.replaceFirst(targetEnv, "")
                                    .replaceAll("//", "/");
                            rewrite = true;
                        }
                        request.setAttribute(
                                GlobalState.HTTP_ACCESS_TOKEN_ATTRIBUTE, access);
                    }
                    catch (Exception e) {
                        if(e instanceof GeneralSecurityException
                                || (e instanceof RuntimeException && e
                                        .getCause() != null
                                        & e.getCause() instanceof GeneralSecurityException)) {}
                        else {
                            throw Throwables.propagate(e);
                        }
                    }
                }
            }
            if(rewrite) {
                request.setAttribute(GlobalState.HTTP_ENVIRONMENT_ATTRIBUTE,
                        targetEnv);
                Reflection.set("_requestURI", target, request);
                Reflection.set("_pathInfo", target, request);
                HttpURI uri = Reflection.get("_uri", request);
                Reflection.set("_rawString", target, uri);
            }
        }
        else {
            String token = findCookieValue(GlobalState.HTTP_AUTH_TOKEN_COOKIE,
                    request);
            if(token != null) {
                try {
                    Object[] auth = decodeAuthToken(token);
                    AccessToken access = (AccessToken) auth[0];
                    request.setAttribute(
                            GlobalState.HTTP_ACCESS_TOKEN_ATTRIBUTE, access);
                }
                catch (Exception e) {
                    if(e instanceof GeneralSecurityException
                            || (e instanceof RuntimeException && e.getCause() != null
                                    & e.getCause() instanceof GeneralSecurityException)) {}
                    else {
                        throw Throwables.propagate(e);
                    }
                }
            }
        }
    }
}
