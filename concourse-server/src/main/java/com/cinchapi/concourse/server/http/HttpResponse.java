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
package com.cinchapi.concourse.server.http;

/**
 * Defines all the actions that can be taken on a response generated by an
 * {@link EndpointContainer}.
 * 
 * @author Jeff Nelson
 */
public interface HttpResponse {

    public String body();

    /**
     * Sets the body
     */
    public void body(String body);

    /**
     * Adds not persistent cookie to the response.
     * Can be invoked multiple times to insert more than one cookie.
     * 
     * @param name name of the cookie
     * @param value value of the cookie
     */
    public void cookie(String name, String value);

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
    public void cookie(String name, String value, int maxAge);

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
    public void cookie(String name, String value, int maxAge, boolean secured);

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
            boolean secured);

    /**
     * Adds/Sets a response header
     */
    public void header(String header, String value);

    /**
     * Trigger a browser redirect
     * 
     * @param location Where to redirect
     */
    public void redirect(String location);

    /**
     * Trigger a browser redirect with specific http 3XX status code.
     *
     * @param location Where to redirect permanently
     * @param httpStatusCode the http status code
     */
    public void redirect(String location, int httpStatusCode);

    /**
     * Removes the cookie.
     * 
     * @param name name of the cookie
     */
    public void removeCookie(String name);

    /**
     * Sets the status code for the response
     */
    public void status(int statusCode);

    /**
     * Sets the content type for the response
     */
    public void type(String contentType);

}
