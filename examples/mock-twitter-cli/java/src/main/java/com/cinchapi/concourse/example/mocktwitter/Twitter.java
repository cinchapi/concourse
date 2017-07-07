/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2017 Jeff Nelson, Cinchapi Software Collective
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
package com.cinchapi.concourse.example.mocktwitter;

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