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
package com.cinchapi.concourse.server.http.webserver;

import java.io.IOException;
import java.security.GeneralSecurityException;

import javax.annotation.Nullable;
import javax.servlet.Filter;
import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.http.HttpURI;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.session.SessionHandler;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.http.HttpAuthToken;
import com.cinchapi.concourse.server.http.HttpRequests;
import com.cinchapi.concourse.util.ObjectUtils;
import com.cinchapi.concourse.util.Strings;
import com.google.common.base.Throwables;

import spark.webserver.NotConsumedException;

/**
 * Simple Jetty Handler
 *
 * @author Per Wendel
 * @author Jeff Nelson
 */
public class ConcourseHttpHandler extends SessionHandler {

    /**
     * Search through the {@code request} for the value of the {@code name}
     * cookie, if it exists.
     * 
     * @param name
     * @param request
     * @return the cookie value or {@code null}
     */
    @Nullable
    private static String findCookieValue(String name,
            HttpServletRequest request) {
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
    private static void rewrite(String target, Request baseRequest,
            HttpServletRequest request) {
        String[] targetParts = target.split("/");
        boolean rewrite = false;
        boolean requireAuth = true;
        if(targetParts.length >= 2) {
            String targetEnv = targetParts[1];
            if(targetEnv.equals("login")) {
                // When route is /login, do not rewrite by dropping the declared
                // environment. Just set the request attribute to use the
                // DEFAULT ENVIRONMENT
                targetEnv = GlobalState.DEFAULT_ENVIRONMENT;
                requireAuth = false;
            }
            else if(targetParts.length >= 3 && targetParts[2].equals("login")) {
                // When route is /<environment>/login, rewrite without a
                // declared environment like we would all other requests, but
                // tell the request attribute to use the declared environment
                // instead of the one in the cookie.
                target = target.replaceFirst(targetEnv, "").replaceAll("//",
                        "/");
                rewrite = true;
                requireAuth = false;
            }
            else {
                // Rewrite all requests to drop the declared environment from
                // the path and use the request attributes to specify meta
                // information
                String token = ObjectUtils.firstNonNullOrNull(
                        findCookieValue(GlobalState.HTTP_AUTH_TOKEN_COOKIE,
                                request), request
                                .getHeader(GlobalState.HTTP_AUTH_TOKEN_HEADER));

                if(token != null) {
                    try {
                        HttpAuthToken auth = HttpRequests
                                .decodeAuthToken(token);
                        if(auth.getEnvironment().equals(targetEnv)) {
                            target = target.replaceFirst(targetEnv, "")
                                    .replaceAll("//", "/");
                            rewrite = true;
                        }
                        else {
                            targetEnv = auth.getEnvironment();
                            rewrite = true;
                        }
                        request.setAttribute(
                                GlobalState.HTTP_ACCESS_TOKEN_ATTRIBUTE,
                                auth.getAccessToken());
                        request.setAttribute(
                                GlobalState.HTTP_FINGERPRINT_ATTRIBUTE,
                                auth.getFingerprint());

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
            String token = ObjectUtils
                    .firstNonNullOrNull(
                            findCookieValue(GlobalState.HTTP_AUTH_TOKEN_COOKIE,
                                    request),
                            request.getHeader(GlobalState.HTTP_AUTH_TOKEN_HEADER));
            if(token != null) {
                try {
                    HttpAuthToken auth = HttpRequests.decodeAuthToken(token);
                    request.setAttribute(
                            GlobalState.HTTP_ACCESS_TOKEN_ATTRIBUTE,
                            auth.getAccessToken());
                    request.setAttribute(
                            GlobalState.HTTP_ENVIRONMENT_ATTRIBUTE,
                            auth.getEnvironment());
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
        // Get the transaction token
        String transaction = findCookieValue(
                GlobalState.HTTP_TRANSACTION_TOKEN_COOKIE, request);
        if(transaction != null) {
            request.setAttribute(GlobalState.HTTP_TRANSACTION_TOKEN_ATTRIBUTE,
                    transaction);
        }
        request.setAttribute(GlobalState.HTTP_REQUIRE_AUTH_ATTRIBUTE,
                requireAuth);
    }

    /**
     * HTTP Access-Control-Allow-Headers header.
     */
    private static String HEADER_ACCESS_CONTROL_ALLOW_HEADERS = "Access-Control-Allow-Headers";

    /**
     * HTTP Access-Control-Allow-Methods header.
     */
    private static String HEADER_ACCESS_CONTROL_ALLOW_METHODS = "Access-Control-Allow-Methods";

    /**
     * HTTP Access-Control-Allow-Origin header.
     */
    private static String HEADER_ACCESS_CONTROL_ALLOW_ORIGIN = "Access-Control-Allow-Origin";

    /**
     * HTTP Vary header.
     */
    private static String HEADER_VARY = "Vary";

    private Filter filter;

    public ConcourseHttpHandler(Filter filter) {
        this.filter = filter;
    }

    @Override
    public void doHandle(String target, Request baseRequest,
            HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {
        try {
            rewrite(target, baseRequest, request);
            if(GlobalState.HTTP_ENABLE_CORS) { // CON-475: Support CORS
                if(GlobalState.HTTP_CORS_DEFAULT_ALLOW_ORIGIN.equals("*")) {
                    response.addHeader("Access-Control-Allow-Headers", "*");
                }
                else {
                    String requestOrigin = request.getHeader("Origin");
                    if(Strings.isSubString(requestOrigin,
                            GlobalState.HTTP_CORS_DEFAULT_ALLOW_ORIGIN)) {
                        response.addHeader(HEADER_ACCESS_CONTROL_ALLOW_ORIGIN,
                                requestOrigin);
                        response.addHeader(HEADER_VARY, requestOrigin);
                    }
                }
                if(GlobalState.HTTP_CORS_DEFAULT_ALLOW_HEADERS.equals("*")) {
                    response.addHeader(HEADER_ACCESS_CONTROL_ALLOW_HEADERS,
                            request.getHeader("Access-Control-Request-Headers"));
                }
                else {
                    response.addHeader(HEADER_ACCESS_CONTROL_ALLOW_HEADERS,
                            GlobalState.HTTP_CORS_DEFAULT_ALLOW_HEADERS);
                }
                String requestMethod = request.getMethod();
                if(GlobalState.HTTP_CORS_DEFAULT_ALLOW_METHODS.equals("*")) {
                    response.addHeader(HEADER_ACCESS_CONTROL_ALLOW_METHODS,
                            requestMethod);
                }
                else {
                    response.addHeader(HEADER_ACCESS_CONTROL_ALLOW_METHODS,
                            GlobalState.HTTP_CORS_DEFAULT_ALLOW_METHODS);
                }
            }
            filter.doFilter(request, response, null);
            baseRequest.setHandled(true);
        }
        catch (NotConsumedException ignore) {
            // TODO : Not use an exception in order to be faster.
            baseRequest.setHandled(false);
        }
    }
}