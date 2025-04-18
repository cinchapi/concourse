/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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
package spark;

import java.io.IOException;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletResponse;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.concourse.server.http.HttpResponse;

/**
 * Provides functionality for modifying the response
 *
 * @author Per Wendel
 */
public class Response implements HttpResponse {

    private HttpServletResponse response;
    private String body;

    protected Response() {
        // Used by wrapper
    }

    Response(HttpServletResponse response) {
        this.response = response;
    }

    /**
     * Sets the status code for the response
     */
    public void status(int statusCode) {
        response.setStatus(statusCode);
    }

    /**
     * Sets the content type for the response
     */
    public void type(String contentType) {
        response.setContentType(contentType);
    }

    /**
     * Sets the body
     */
    public void body(String body) {
        this.body = body;
    }

    public String body() {
        return this.body;
    }

    /**
     * Gets the raw response object handed in by Jetty
     */
    public HttpServletResponse raw() {
        return response;
    }

    /**
     * Trigger a browser redirect
     * 
     * @param location Where to redirect
     */
    public void redirect(String location) {
        try {
            response.sendRedirect(location);
        }
        catch (IOException ioException) {
            throw CheckedExceptions.wrapAsRuntimeException(ioException);
        }
    }

    /**
     * Trigger a browser redirect with specific http 3XX status code.
     *
     * @param location Where to redirect permanently
     * @param httpStatusCode the http status code
     */
    public void redirect(String location, int httpStatusCode) {
        response.setStatus(httpStatusCode);
        response.setHeader("Location", location);
        response.setHeader("Connection", "close");
        try {
            response.sendError(httpStatusCode);
        }
        catch (IOException e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
    }

    /**
     * Adds/Sets a response header
     */
    public void header(String header, String value) {
        response.addHeader(header, value);
    }

    /**
     * Adds not persistent cookie to the response.
     * Can be invoked multiple times to insert more than one cookie.
     * 
     * @param name name of the cookie
     * @param value value of the cookie
     */
    public void cookie(String name, String value) {
        cookie(name, value, -1, false);
    }

    /**
     * Adds cookie to the response. Can be invoked multiple times to insert more
     * than one cookie.
     * 
     * @param name name of the cookie
     * @param value value of the cookie
     * @param maxAge max age of the cookie in seconds (negative for the not
     *            persistent cookie,
     *            zero - deletes the cookie)
     */
    public void cookie(String name, String value, int maxAge) {
        cookie(name, value, maxAge, false);
    }

    /**
     * Adds cookie to the response. Can be invoked multiple times to insert more
     * than one cookie.
     *
     * @param name name of the cookie
     * @param value value of the cookie
     * @param maxAge max age of the cookie in seconds (negative for the not
     *            persistent cookie, zero - deletes the cookie)
     * @param secured if true : cookie will be secured
     *            zero - deletes the cookie)
     */
    public void cookie(String name, String value, int maxAge, boolean secured) {
        cookie("", name, value, maxAge, secured);
    }

    /**
     * Adds cookie to the response. Can be invoked multiple times to insert more
     * than one cookie.
     *
     * @param path path of the cookie
     * @param name name of the cookie
     * @param value value of the cookie
     * @param maxAge max age of the cookie in seconds (negative for the not
     *            persistent cookie, zero - deletes the cookie)
     * @param secured if true : cookie will be secured
     *            zero - deletes the cookie)
     */
    public void cookie(String path, String name, String value, int maxAge,
            boolean secured) {
        Cookie cookie = new Cookie(name, value);
        cookie.setPath(path);
        cookie.setMaxAge(maxAge);
        cookie.setSecure(secured);
        response.addCookie(cookie);
    }

    /**
     * Removes the cookie.
     * 
     * @param name name of the cookie
     */
    public void removeCookie(String name) {
        Cookie cookie = new Cookie(name, "");
        cookie.setMaxAge(0);
        response.addCookie(cookie);
    }
}
