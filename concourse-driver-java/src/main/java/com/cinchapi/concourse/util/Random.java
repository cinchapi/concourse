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
package com.cinchapi.concourse.util;

import com.cinchapi.concourse.util.RandomStringGenerator;
import com.google.common.base.Strings;

/**
 * Random generators.
 * 
 * @author Jeff Nelson
 */
public abstract class Random {

    private static final java.util.Random rand = new java.util.Random();
    private static final RandomStringGenerator strand = new RandomStringGenerator();

    /**
     * Return a random boolean value.
     * 
     * @return the boolean.
     */
    public static boolean getBoolean() {
        int seed = rand.nextInt();
        if(seed % 2 == 0) {
            return true;
        }
        else {
            return false;
        }
    }

    /**
     * Return a random double value.
     * 
     * @return the double.
     */
    public static double getDouble() {
        return rand.nextDouble();
    }

    /**
     * Return a random float value.
     * 
     * @return the float.
     */
    public static float getFloat() {
        return rand.nextFloat();
    }

    /**
     * Return a random integer value.
     * 
     * @return the int.
     */
    public static int getInt() {
        return rand.nextInt();
    }

    /**
     * Return a random long value.
     * 
     * @return the long.
     */
    public static long getLong() {
        return rand.nextLong();
    }

    /**
     * Return a random negative number.
     * 
     * @return the number.
     */
    public static Number getNegativeNumber() {
        int seed = getInt();
        if(seed % 5 == 0) {
            return (float) -1 * Math.abs(getFloat());
        }
        else if(seed % 4 == 0) {
            return (double) -1 * Math.abs(getDouble());
        }
        else if(seed % 3 == 0) {
            return (long) -1 * Math.abs(getLong());
        }
        else {
            return (int) -1 * Math.abs(getInt());
        }
    }

    /**
     * Return a random number value.
     * 
     * @return the number
     */
    public static Number getNumber() {
        int seed = getInt();
        if(seed % 5 == 0) {
            return getFloat();
        }
        else if(seed % 4 == 0) {
            return getDouble();
        }
        else if(seed % 3 == 0) {
            return getLong();
        }
        else {
            return getInt();
        }
    }

    /**
     * Return a random object value.
     * 
     * @return the object
     */
    public static Object getObject() {
        int seed = rand.nextInt();
        if(seed % 5 == 0) {
            return getBoolean();
        }
        else if(seed % 2 == 0) {
            return getNumber();
        }
        else {
            return getString();
        }
    }

    /**
     * Return a random positive long value.
     * 
     * @return the long
     */
    public static Number getPositiveNumber() {
        int seed = getInt();
        if(seed % 5 == 0) {
            return Math.abs(getFloat());
        }
        else if(seed % 4 == 0) {
            return (double) Math.abs(getDouble());
        }
        else if(seed % 3 == 0) {
            return (long) Math.abs(getLong());
        }
        else {
            return (int) Math.abs(getInt());
        }
    }

    /**
     * Pause execution for a random number of milliseconds between 0 and 1000 (1
     * second).
     */
    public static void sleep() {
        try {
            Thread.sleep(rand.nextInt(1000) + 1); // between 0 and 1 second
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Pause execution for a random number of milliseconds between 100 and 200.
     */
    public static void tinySleep() {
        try {
            Thread.sleep(rand.nextInt(200) + 100); // between a and 100 ms
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static int getScaleCount() {
        return rand.nextInt(90) + 10;
    }

    /**
     * Return a string, possibly with digits.
     * 
     * @return the string
     */
    public static String getString() {
        String string = null;
        while (Strings.nullToEmpty(string).trim().isEmpty()) {
            string = strand.nextStringAllowDigits();
        }
        return string;
    }

    /**
     * Return a simple string with no digits or spaces.
     * 
     * @return the string
     */
    public static String getSimpleString() {
        String string = null;
        while (Strings.nullToEmpty(string).trim().isEmpty()) {
            string = getStringNoDigits().replaceAll(" ", "");
        }
        return string;
    }

    /**
     * Return a get string, with no digits.
     * 
     * @return the string.
     */
    public static String getStringNoDigits() {
        return strand.nextString();
    }

}
