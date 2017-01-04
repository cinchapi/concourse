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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * A random string generator.
 * 
 * @author Jeff Nelson
 */
public class RandomStringGenerator {

    Random random;

    private static final int MAX_DISTRIBUTION_OF_SPACE_CHARS = 5;
    private static final int MAX_RANDOM_STRING_LENGTH = 100;
    private static final char spaceChar = (char) ' ';
    private static final Character[] digits = new Character[10];
    private static final Character[] letters = new Character[26];

    static {
        for (int i = 0; i < digits.length; ++i) {
            digits[i] = (char) ('0' + i);
        }
        for (int i = 0; i < letters.length; ++i) {
            letters[i] = (char) ('a' + i);
        }
    }

    /**
     * Create a new generator with a seed that is very likely to be distinct
     * from any other invocation of this constructor.
     */
    public RandomStringGenerator() {
        this(0);
    }

    /**
     * Create a new generator with the specified seed.
     * 
     * @param seed
     */
    public RandomStringGenerator(long seed) {
        random = seed == 0 ? new Random() : new Random(seed);
    }

    /**
     * Generate a string of random length and no digits
     * 
     * @return random string
     */
    public String nextString() {
        return nextString(random.nextInt(MAX_RANDOM_STRING_LENGTH) + 1);
    }

    /**
     * Generate a string of the specified length and no digits
     * 
     * @param length
     * @return random string
     */
    public String nextString(int length) {
        Character[] spaces = new Character[random
                .nextInt(MAX_DISTRIBUTION_OF_SPACE_CHARS)];
        for (int i = 0; i < spaces.length; ++i) {
            spaces[i] = spaceChar;
        }
        ArrayList<Character> source = new ArrayList<Character>();
        source.addAll(Arrays.asList(letters));
        source.addAll(Arrays.asList(spaces));
        Collections.shuffle(source);
        return build(length, source);
    }

    /**
     * Generate a string of random length, possibly with digits
     * 
     * @return random string
     */
    public String nextStringAllowDigits() {
        return nextStringAllowDigits(random.nextInt(MAX_RANDOM_STRING_LENGTH) + 1);
    }

    /**
     * Generate a string of the specified length, possibly with digits
     * 
     * @param length
     * @return random string
     */
    public String nextStringAllowDigits(int length) {
        Character[] spaces = new Character[random
                .nextInt(MAX_DISTRIBUTION_OF_SPACE_CHARS)];
        for (int i = 0; i < spaces.length; ++i) {
            spaces[i] = spaceChar;
        }
        ArrayList<Character> source = new ArrayList<Character>();
        source.addAll(Arrays.asList(letters));
        source.addAll(Arrays.asList(digits));
        source.addAll(Arrays.asList(spaces));
        Collections.shuffle(source);
        return build(length, source);
    }

    /**
     * Build a random string of {@code length} using the characters in
     * {@code source}
     * 
     * @param length
     * @param source
     * @return random string
     */
    private String build(int length, List<Character> source) {
        if(length < 1) {
            throw new IllegalArgumentException(
                    "Cannot generate a string with fewer than 1 characters.");
        }
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < length; ++i) {
            int index = random.nextInt(source.size());
            builder.append(source.get(index));
        }
        return builder.toString().trim();
    }

}
