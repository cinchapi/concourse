/*
 * Copyright 2011- Per Wendel
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package spark.webserver;

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

import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.http.HttpRequests;
import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.util.ObjectUtils;
import com.cinchapi.concourse.util.Reflection;
import com.google.common.base.MoreObjects;
import com.google.common.base.Throwables;

import spark.webserver.JettyHandler;
import spark.webserver.NotConsumedException;

/**
 * Simple Jetty Handler
 *
 * @author Per Wendel
 */
class JettyHandler extends SessionHandler {

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
                // Do not rewrite login with no declared environment. Just set
                // the request attribute to use the DEFAULT ENVIRONMENT
                targetEnv = GlobalState.DEFAULT_ENVIRONMENT;
                requireAuth = false;
            }
            else if(targetParts.length >= 3 && targetParts[2].equals("login")) {
                // Rewrite login with declared environment like we would all
                // other requests, but tell the request attribute to use the
                // declared environment instead of the one in the cookie.
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
                        Object[] auth = HttpRequests.decodeAuthToken(token);
                        AccessToken access = (AccessToken) auth[0];
                        String authEnv = (String) auth[1];
                        String fingerprint = (String) auth[2];
                        if(authEnv.equals(targetEnv)) {
                            target = target.replaceFirst(targetEnv, "")
                                    .replaceAll("//", "/");
                            rewrite = true;
                        }
                        else {
                            targetEnv = authEnv;
                            rewrite = true;
                        }
                        request.setAttribute(
                                GlobalState.HTTP_ACCESS_TOKEN_ATTRIBUTE, access);
                        request.setAttribute(
                                GlobalState.HTTP_FINGERPRINT_ATTRIBUTE,
                                fingerprint);

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
            String token = MoreObjects
                    .firstNonNull(
                            findCookieValue(GlobalState.HTTP_AUTH_TOKEN_COOKIE,
                                    request),
                            request.getHeader(GlobalState.HTTP_AUTH_TOKEN_HEADER));
            if(token != null) {
                try {
                    Object[] auth = HttpRequests.decodeAuthToken(token);
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

    private Filter filter;

    public JettyHandler(Filter filter) {
        this.filter = filter;
    }

    @Override
    public void doHandle(String target, Request baseRequest,
            HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {
        try {
            rewrite(target, baseRequest, request);
            filter.doFilter(request, response, null);
            baseRequest.setHandled(true);
        }
        catch (NotConsumedException ignore) {
            // TODO : Not use an exception in order to be faster.
            baseRequest.setHandled(false);
        }
    }

}