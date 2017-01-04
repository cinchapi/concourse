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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.Link;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.time.Time;

/**
 * This class mimics a subset of the functionality of Twitter, backed by a
 * {@link Concourse} database. This class is only intended to demo some of the
 * functionality in Concourse and show off the simplicity of programming against
 * the schemaless database. DO NOT use this class in a real application.
 * 
 * @author Jeff
 */
public class ConcourseTwitter implements Twitter {

    /**
     * Return a string that represents the hash for {@code string}.
     * 
     * @param string
     * @return the hash
     */
    private static String hash(String string) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(string.getBytes());
            // props to http://stackoverflow.com/a/5470268/1336833
            StringBuilder hex = new StringBuilder();
            for (int i = 0; i < hash.length; i++) {
                if((0xff & hash[i]) < 0x10) {
                    hex.append("0" + Integer.toHexString((0xFF & hash[i])));
                }
                else {
                    hex.append(Integer.toHexString(0xFF & hash[i]));
                }
            }
            return hex.toString();
        }
        catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e); // this should never happen
        }
    }

    private static Comparator<Long> REVERSE_CHRONOLOGICAL_SORTER = new Comparator<Long>() {

        @Override
        public int compare(Long o1, Long o2) {
            return -1 * o1.compareTo(o2);
        }

    };

    /**
     * The id of the user that is currenty logged in.
     */
    private long userid = 0;

    /**
     * The connection to Concourse. This assumes that the server is running in
     * the default (localhost:1717) and has the default admin credentials.
     */
    private final Concourse concourse = Concourse.connect();

    /**
     * Generates secure random salt values. This is overkill for the demo, but
     * good to use for real applications.
     */
    private final SecureRandom srand = new SecureRandom();

    @Override
    public boolean follow(String username) {
        long id = getUserId(username);
        if(id != userid) {
            return concourse.link("following", userid, id)
                    && concourse.link("followers", id, userid);
        }
        else {
            return false;
        }
    }

    @Override
    public boolean login(String username, String password) {
        if(exists(username)) {
            long userid = getUserId(username);
            long salt = concourse.get("salt", userid);
            password = hash(password + salt);
            if(password.equals(concourse.get("password", userid))) {
                this.userid = userid;
                return true;
            }
        }
        return false;
    }

    @Override
    public Map<Long, String> mentions() {
        return getTweetInfo(concourse.select("mentions", userid));
    }

    @Override
    public boolean register(String username, String password) {
        if(!exists(username)) {
            long id = Time.now();
            concourse.set("username", username, id);
            long salt = srand.nextLong();
            concourse.set("salt", salt, id);
            concourse.set("password", hash(password + salt), id);
            return true;
        }
        else {
            throw new IllegalArgumentException("Username is already taken!");
        }
    }

    @Override
    public Map<Long, String> timeline() {
        Map<Long, String> timeline = new TreeMap<Long, String>(
                REVERSE_CHRONOLOGICAL_SORTER);

        // Get all of my tweets
        timeline.putAll(getTweetInfo(concourse.select("tweets", userid)));

        // Get all of the tweets for users i am following
        Set<Object> following = concourse.select("following", userid);
        for (Object link : following) {
            long id = ((Link) link).longValue();
            timeline.putAll(getTweetInfo(concourse.select("tweets", id)));
        }

        // Get all the tweets where I am mentioned
        timeline.putAll(mentions());

        return timeline;
    }

    @Override
    public void tweet(String message) {
        if(message.length() <= 140) {
            long tweetId = Time.now();
            concourse.set("message", message, tweetId);
            concourse.set("timestamp", Time.now(), tweetId);

            // Link the tweet to the author
            concourse.link("author", tweetId, userid);

            // Link the author to the tweet
            concourse.link("tweets", userid, tweetId);

            // Parse out mentioned users and link them to the tweet
            Pattern pattern = Pattern.compile("[@][\\w]+");
            Matcher matcher = pattern.matcher(message);
            while (matcher.find()) {
                try {
                    String uname = matcher.group().replace("@", "");
                    long mentioned = getUserId(uname);
                    concourse.link("mentions", mentioned, tweetId);
                }
                catch (IllegalArgumentException e) {
                    continue; // uname does not exist
                }
            }
        }
        else {
            throw new IllegalArgumentException("Tweet is too long!");
        }

    }

    @Override
    public boolean unfollow(String username) {
        long id = getUserId(username);
        return concourse.unlink("following", userid, id)
                && concourse.unlink("followers", id, userid);
    }

    /**
     * Return {@code true} if a user with {@code username} exists.
     * 
     * @param username
     * @return {@code true} if {@code username} exists
     */
    private boolean exists(String username) {
        return !concourse.find("username", Operator.EQUALS, username).isEmpty();
    }

    /**
     * Return a map from timestamp to tweet that contains information for the
     * set of tweet ids specified in {@code tweets}.
     * 
     * @param tweets
     * @return the tweet info
     */
    private Map<Long, String> getTweetInfo(Set<Object> tweets) {
        Map<Long, String> collection = new TreeMap<Long, String>(
                REVERSE_CHRONOLOGICAL_SORTER);
        for (Object link : tweets) {
            long id = ((Link) link).longValue();
            long timestamp = concourse.get("timestamp", id);
            String author = concourse.get("username",
                    concourse.<Link> get("author", id).longValue());
            String message = concourse.get("message", id);
            collection.put(timestamp, author + ": " + message);
        }
        return collection;
    }

    /**
     * Return the user id (primary key) for the user with {@code username}.
     * 
     * @param username
     * @return the user id for {@code username}
     * @throws IllegalArgumentException
     */
    private long getUserId(String username) {
        if(exists(username)) {
            return concourse.find("username", Operator.EQUALS, username)
                    .iterator().next();
        }
        else {
            throw new IllegalArgumentException("Invalid user");
        }

    }

}