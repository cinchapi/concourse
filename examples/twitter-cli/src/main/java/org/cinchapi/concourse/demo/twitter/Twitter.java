/*
* Copyright (c) 2013-2015 Cinchapi Inc.
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
package org.cinchapi.concourse.demo.twitter;

import java.util.Map;

/**
 * The interface for the back end of an command line application that mimics
 * some of the functionality of Twitter. The implementing class is expected to
 * keep track of the current user session.
 *
 * @author Jeff Nelson
 */
public interface Twitter {

    /**
     * Follow {@code username}.
     *
     * @param username
     * @return {@code true} if the current user starts following
     *         {@code username}.
     */
    public boolean follow(String username);

    /**
     * Authenticate the {@code username} and {@code password}.
     *
     * @param username
     * @param password
     * @return {@code true} if the login is successful
     */
    public boolean login(String username, String password);

    /**
     * Register a user identified by the {@code username} and {@code password}
     * combination.
     *
     * @param username
     * @param password
     * @return {@code true} if the new user is registered successfully
     */
    public boolean register(String username, String password);

    /**
     * Return the tweets where the current user is mentioned.
     *
     * @return my mentions
     */
    public Map<Long, String> mentions();

    /**
     * Return the tweets on the current user's timeline.
     *
     * @return my timeline
     */
    public Map<Long, String> timeline();

    /**
     * Post a tweet with {@code message}.
     *
     * @param message
     */
    public void tweet(String message);

    /**
     * Unfollow {@code username}.
     *
     * @param username
     * @return {@code true} if the current user starts unfollowing
     *         {@code username}
     */
    public boolean unfollow(String username);

}
