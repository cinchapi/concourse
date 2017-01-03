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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.cinchapi.concourse.time.Time;

/**
 * 
 * 
 * @author Jeff Nelson
 */
public class MysqlTwitter implements Twitter {

    private static Comparator<Long> REVERSE_CHRONOLOGICAL_SORTER = new Comparator<Long>() {

        @Override
        public int compare(Long o1, Long o2) {
            return -1 * o1.compareTo(o2);
        }

    };

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

    /**
     * Generates secure random salt values. This is overkill for the demo, but
     * good to use for real applications.
     */
    private final SecureRandom srand = new SecureRandom();

    /**
     * The id of the user that is currenty logged in.
     */
    private long userid = 0;

    private Connection mysql;
    {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            String user = "root";
            String password = "root";
            String url = "jdbc:mysql://localhost:8889/twitter";
            mysql = DriverManager.getConnection(url, user, password);
        }
        catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
            mysql = null;
        }
    }

    @Override
    public boolean follow(String username) {
        try {
            long id = getUserId(username);
            if(id != userid) {
                PreparedStatement stmt = mysql
                        .prepareStatement("INSERT INTO followers (follower, followed) VALUES (?,?)");
                stmt.setLong(1, userid);
                stmt.setLong(2, id);
                return stmt.executeUpdate() > 0;
            }
            else {
                return false;
            }
        }
        catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean login(String username, String password) {
        try {
            if(exists(username)) {
                long userid = getUserId(username);
                PreparedStatement stmt = mysql
                        .prepareStatement("SELECT salt, password FROM users WHERE uid = ?");
                stmt.setLong(1, userid);
                stmt.execute();
                ResultSet result = stmt.getResultSet();
                result.next();
                long salt = result.getLong(1);
                String pword = result.getString(2);
                password = hash(password + salt);
                if(password.equals(pword)) {
                    this.userid = userid;
                    return true;
                }
            }
            return false;
        }
        catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public Map<Long, String> mentions() {
        try {
            PreparedStatement stmt = mysql
                    .prepareStatement("SELECT tid FROM mentions WHERE uid = ?");
            stmt.setLong(1, userid);
            stmt.execute();
            ResultSet result = stmt.getResultSet();
            Set<Long> tids = new HashSet<Long>();
            while (result.next()) {
                tids.add(result.getLong(1));
            }
            return getTweetInfo(tids);
        }
        catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public boolean register(String username, String password) {
        try {
            if(!exists(username)) {
                long id = Time.now();
                long salt = srand.nextLong();
                PreparedStatement stmt = mysql
                        .prepareStatement("INSERT INTO users (uid, username, password, salt) VALUES (?,?,?,?)");
                stmt.setLong(1, id);
                stmt.setString(2, username);
                stmt.setString(3, hash(password + salt));
                stmt.setLong(4, salt);
                return stmt.executeUpdate() > 0;
            }
            else {
                throw new IllegalArgumentException("Username is already taken!");
            }
        }
        catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public Map<Long, String> timeline() {
        try {
            Map<Long, String> timeline = new TreeMap<Long, String>(
                    REVERSE_CHRONOLOGICAL_SORTER);

            // Get all of my tweets
            PreparedStatement stmt = mysql
                    .prepareStatement("SELECT tid FROM tweets where author = ?");
            stmt.setLong(1, userid);
            stmt.execute();
            ResultSet result = stmt.getResultSet();
            Set<Long> tids = new HashSet<Long>();
            while (result.next()) {
                tids.add(result.getLong(1));
            }
            timeline.putAll(getTweetInfo(tids));

            // Get all of the tweets for users i am following
            stmt = mysql
                    .prepareStatement("SELECT followed FROM followers WHERE follower = ?");
            stmt.setLong(1, userid);
            stmt.execute();
            result = stmt.getResultSet();
            while (result.next()) {
                long followed = result.getLong(1);
                PreparedStatement stmt2 = mysql
                        .prepareStatement("SELECT tid FROM tweets where author = ?");
                stmt2.setLong(1, followed);
                stmt2.execute();
                ResultSet result2 = stmt.getResultSet();
                Set<Long> tids2 = new HashSet<Long>();
                while (result2.next()) {
                    tids2.add(result2.getLong(1));
                }
                timeline.putAll(getTweetInfo(tids2));
            }

            // Get all the tweets where I am mentioned
            timeline.putAll(mentions());

            return timeline;
        }
        catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void tweet(String message) {
        try {
            if(message.length() <= 140) {
                long tweetId = Time.now();

                PreparedStatement stmt = mysql
                        .prepareStatement("INSERT INTO tweets (tid, author, message, timestamp) VALUES (?,?,?,?)");
                stmt.setLong(1, tweetId);
                stmt.setLong(2, userid);
                stmt.setString(3, message);
                stmt.setLong(4, Time.now());
                stmt.executeUpdate();

                // Parse out mentioned users and link them to the tweet
                Pattern pattern = Pattern.compile("[@][\\w]+");
                Matcher matcher = pattern.matcher(message);
                PreparedStatement stmt2 = mysql
                        .prepareStatement("INSERT INTO mentions (tid, uid) VALUES (?,?)");
                while (matcher.find()) {
                    String uname = matcher.group().replace("@", "");
                    long mentioned = getUserId(uname);
                    stmt2.clearParameters();
                    stmt2.setLong(1, tweetId);
                    stmt2.setLong(2, mentioned);
                    stmt2.execute();
                }
            }
            else {
                throw new IllegalArgumentException("Tweet is too long!");
            }
        }
        catch (SQLException e) {
            e.printStackTrace();
        }

    }

    @Override
    public boolean unfollow(String username) {
        try {
            long id = getUserId(username);
            if(id != userid) {
                PreparedStatement stmt = mysql
                        .prepareStatement("DELETE FROM folowers WHERE follower = ? AND followerd = ?");
                stmt.setLong(1, userid);
                stmt.setLong(2, id);
                return stmt.executeUpdate() > 0;
            }
            else {
                return false;
            }
        }
        catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Return {@code true} if a user with {@code username} exists.
     * 
     * @param username
     * @return {@code true} if {@code username} exists
     * @throws SQLException
     */
    private boolean exists(String username) throws SQLException {
        PreparedStatement stmt = mysql
                .prepareStatement("SELECT uid FROM users WHERE username = ?");
        stmt.setString(1, username);
        stmt.execute();
        ResultSet result = stmt.getResultSet();
        return result.next();
    }

    /**
     * Return a map from timestamp to tweet that contains information for the
     * set of tweet ids specified in {@code tweets}.
     * 
     * @param tweets
     * @return the tweet info
     */
    private Map<Long, String> getTweetInfo(Set<Long> tweets) {
        try {
            Map<Long, String> collection = new TreeMap<Long, String>(
                    REVERSE_CHRONOLOGICAL_SORTER);
            PreparedStatement stmt = mysql
                    .prepareStatement("SELECT username, message, timestamp FROM tweets JOIN users on author = uid WHERE tid = ?");
            for (Long tweetId : tweets) {
                stmt.clearParameters();
                stmt.setLong(1, tweetId);
                stmt.execute();
                ResultSet result = stmt.getResultSet();
                result.next();
                String author = result.getString(1);
                String message = result.getString(2);
                long timestamp = result.getLong(3);
                collection.put(timestamp, author + ": " + message);
            }
            return collection;
        }
        catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Return the user id (primary key) for the user with {@code username}.
     * 
     * @param username
     * @return the user id for {@code username}
     * @throws SQLException
     * @throws IllegalArgumentException
     */
    private long getUserId(String username) throws SQLException {
        if(exists(username)) {
            PreparedStatement stmt = mysql
                    .prepareStatement("SELECT uid FROM users WHERE username = ?");
            stmt.setString(1, username);
            stmt.execute();
            ResultSet result = stmt.getResultSet();
            result.next();
            return result.getLong(1);
        }
        else {
            throw new IllegalArgumentException("Invalid user");
        }

    }
}