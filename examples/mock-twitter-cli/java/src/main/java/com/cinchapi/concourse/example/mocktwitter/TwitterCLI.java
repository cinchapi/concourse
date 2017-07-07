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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import com.cinchapi.concourse.time.Time;

/**
 * The TwitterCLI is a command line application that mimics some of the
 * functionality of Twitter. This application is designed to run with a
 * {@link Twitter} back end that uses Concourse or some other database for data
 * storage.
 * 
 * @author Jeff Nelson
 */
public class TwitterCLI {

    /**
     * The {@link Twitter} back end that is used to manage the application logic
     * and data storage.
     */
    public static final Twitter twitter = new ConcourseTwitter();

    /**
     * Run the program...
     * 
     * @param args
     */
    public static void main(String... args) {

        String linebreak = System.getProperty("line.separator");
        boolean loggedIn = false;
        boolean running = true;
        String loggedOutInstructions = new StringBuilder()
                .append("Type 'login <username>' to login").append(linebreak)
                .append("Type 'register <username>' to create user")
                .append(linebreak).append("Type 'exit' to quit").toString();
        String loggedInInstructions = new StringBuilder()
                .append("Type 'tweet <message>' to tweet").append(linebreak)
                .append("Type 'follow <username>' to follow user")
                .append(linebreak)
                .append("Type 'unfollow <username>' to unfollow user")
                .append(linebreak)
                .append("Type 'timeline' to view your timeline")
                .append(linebreak)
                .append("Type 'mentions' to view your mentions")
                .append(linebreak).append("Type 'exit' to quit").toString();
        System.out.println("Welcome to the Twitter Demo application!");
        System.out.println("THIS APPLICATION READS PASSWORDS IN PLAIN TEXT SO "
                + "PLEASE DON'T USE SENSITIVE INFORMATION DURING TESTING");
        while (running) {
            try {
                if(!loggedIn) {
                    System.out.println("");
                    System.out.println(loggedOutInstructions);
                }
                else {
                    System.out.println("");
                    System.out.println(loggedInInstructions);
                }
                String input = getInput("twitter>");
                String[] toks = input.split(" ");
                if(toks.length < 1) {
                    System.err.println("Please specify an action");
                }
                else {
                    String action = toks[0];
                    if(action.equalsIgnoreCase("login") && !loggedIn) {
                        if(toks.length < 2) {
                            System.err.println("Please specify a username");
                        }
                        else {
                            String username = toks[1];
                            String password = getInput("password>");
                            if(twitter.login(username, password)) {
                                loggedIn = true;
                                System.out.println("Successfully logged in as "
                                        + username);
                            }
                            else {
                                System.err.println("Invalid username/password "
                                        + "combination");
                            }
                        }
                    }
                    else if(action.equalsIgnoreCase("register") && !loggedIn) {
                        if(toks.length < 2) {
                            System.err.println("Please specify a username");
                        }
                        else {
                            String username = toks[1];
                            String password = getInput("password>");
                            if(twitter.register(username, password)) {
                                System.out.println("Successfully registered "
                                        + username);
                            }
                            else {
                                System.err
                                        .println("Could not register user with the "
                                                + "specified username/password combination");
                            }
                        }
                    }
                    else if(action.equalsIgnoreCase("tweet") && loggedIn) {
                        if(toks.length < 2) {
                            System.err
                                    .println("Please specify a message for the tweet");
                        }
                        else {
                            StringBuilder message = new StringBuilder();
                            for (int i = 1; i < toks.length; i++) {
                                if(i != 1) {
                                    message.append(" ");
                                }
                                message.append(toks[i]);
                            }
                            twitter.tweet(message.toString());
                        }
                    }
                    else if(action.equalsIgnoreCase("follow") && loggedIn) {
                        if(toks.length < 2) {
                            System.err.println("Please specify a username");
                        }
                        else if(twitter.follow(toks[1])) {
                            System.out.println("You are now following "
                                    + toks[1]);
                        }
                        else {
                            System.err.println("Error trying to follow "
                                    + toks[1]);
                        }

                    }
                    else if(action.equalsIgnoreCase("unfollow") && loggedIn) {
                        if(toks.length < 2) {
                            System.err.println("Please specify a username");
                        }
                        else if(twitter.follow(toks[1])) {
                            System.out.println("You are no longer following "
                                    + toks[1]);
                        }
                        else {
                            System.err.println("Error trying to unfollow "
                                    + toks[1]);
                        }
                    }
                    else if(action.equalsIgnoreCase("timeline") && loggedIn) {
                        displayTweets(twitter.timeline());
                    }
                    else if(action.equalsIgnoreCase("mentions") && loggedIn) {
                        displayTweets(twitter.mentions());
                    }
                    else if(action.equalsIgnoreCase("exit")) {
                        running = false;
                    }
                    else {
                        System.err.println("Please specify a valid action");
                    }
                }
            }
            catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }
        System.exit(0);

    }

    /**
     * Format and print {@code tweets} to the console.
     * 
     * @param tweets
     */
    private static void displayTweets(Map<Long, String> tweets) {
        for (Entry<Long, String> tweet : tweets.entrySet()) {
            String elapsed = getElapsedTimeString(tweet.getKey());
            String message = tweet.getValue();
            System.out.println(message + " (" + elapsed + " ago)");
        }
    }

    /**
     * Return a string that describes the time that has elapsed since the
     * timestamp specified in {@code microseconds}.
     * 
     * @param microseconds
     * @return the elapsed time string
     */
    private static String getElapsedTimeString(long microseconds) {
        long elapsed = TimeUnit.SECONDS.convert(Time.now() - microseconds,
                TimeUnit.MICROSECONDS);
        String unit = "";
        if(elapsed < 60) {
            unit = "seconds";
        }
        else if(elapsed >= 60 && elapsed < 3600) {
            elapsed = TimeUnit.MINUTES.convert(elapsed, TimeUnit.SECONDS);
            unit = elapsed > 1 ? "minutes" : "minute";
        }
        else if(elapsed >= 3600 && elapsed < 86400) {
            elapsed = TimeUnit.HOURS.convert(elapsed, TimeUnit.SECONDS);
            unit = elapsed > 1 ? "hours" : "hour";
        }
        else {
            elapsed = TimeUnit.DAYS.convert(elapsed, TimeUnit.SECONDS);
            unit = elapsed > 1 ? "days" : "day";
        }
        return elapsed + " " + unit;
    }

    /**
     * Get user input from the command line.
     * 
     * @param prompt
     * @return the input
     */
    private static String getInput(String prompt) {
        try {
            System.out.print(prompt + " ");
            return input.readLine();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // Handler to read System.in
    private static final BufferedReader input = new BufferedReader(
            new InputStreamReader(System.in));

}