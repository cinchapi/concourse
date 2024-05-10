/*
 * Copyright (c) 2013-2024 Cinchapi Inc.
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
package com.cinchapi.concourse.search;

/**
 * A specialized {@link Infingram} that is compiled and optimized for repeated
 * searches within several haystacks.
 * </p>
 *
 * @author Jeff Nelson
 */
public class CompiledInfingram extends Infingram {

    /**
     * Determines if {@code needle[p:end]} is a prefix of {@code needle}.
     * 
     * @param needle the sequence of characters to search for
     * @param p the position to start the prefix check
     * @return {@code true} if {@code needle[p:end]} is a prefix of
     *         {@code needle}
     */
    private static boolean isPrefix(char[] needle, int p) {
        for (int i = p, j = 0; i < needle.length; ++i, ++j) {
            if(needle[i] != needle[j]) {
                return false;
            }
        }
        return true;
    }

    /**
     * Makes the jump table based on the mismatched character information (bad
     * character rule).
     * 
     * @param needle the sequence of characters to search for
     * @return the character table used for the Boyer-Moore algorithm
     */
    private static int[] makeCharTable(char[] needle) {
        int[] table = new int[ALPHABET_SIZE];
        for (int i = 0; i < table.length; ++i) {
            table[i] = needle.length;
        }
        for (int i = 0; i < needle.length; ++i) {
            table[needle[i]] = needle.length - 1 - i;
        }
        return table;
    }

    /**
     * Makes the jump table based on the scan offset where the mismatch occurs
     * (good suffix rule).
     * 
     * @param needle the sequence of characters to search for
     * @return the offset table used for the Boyer-Moore algorithm
     */
    private static int[] makeOffsetTable(char[] needle) {
        int[] table = new int[needle.length];
        int lastPrefixPosition = needle.length;
        for (int i = needle.length; i > 0; --i) {
            if(isPrefix(needle, i)) {
                lastPrefixPosition = i;
            }
            table[needle.length - i] = lastPrefixPosition - i + needle.length;
        }
        for (int i = 0; i < needle.length - 1; ++i) {
            int slen = suffixLength(needle, i);
            table[slen] = needle.length - 1 - i + slen;
        }
        return table;
    }

    /**
     * Returns the maximum length of the substring ending at {@code p} that is
     * also
     * a suffix (good suffix rule).
     * 
     * @param needle the sequence of characters to search for
     * @param p the position to start the suffix check
     * @return the maximum length of the substring ending at {@code p} that is
     *         also a suffix
     */
    private static int suffixLength(char[] needle, int p) {
        int len = 0;
        for (int i = p, j = needle.length - 1; i >= 0
                && needle[i] == needle[j]; --i, --j) {
            len += 1;
        }
        return len;
    }

    /**
     * The size of the alphabet used for the character table.
     */
    private static final int ALPHABET_SIZE = Character.MAX_VALUE + 1;

    /**
     * Handles the "bad character" rule in the Boyer-Moore string search
     * algorithm.
     */
    private final int[] charTable;

    /**
     * Handles the character offsets in the Boyer-Moore string search algorithm.
     */
    private final int[] offsetTable;

    /**
     * Construct a new instance.
     * 
     * @param needle
     */
    public CompiledInfingram(String needle) {
        super(needle);
        if(tokens.length == 1) {
            // If the needle only contains 1 token, we can get away with a
            // straight substring match (e.g., no need to worry about multi word
            // infix matches). Therefore, we can rely on optimized substring
            // searching algorithms like Boyer Moore.
            charTable = makeCharTable(tokens[0]);
            offsetTable = makeOffsetTable(tokens[0]);
        }
        else {
            charTable = null;
            offsetTable = null;
        }
    }

    @Override
    public boolean in(String value) {
        if(charTable != null && offsetTable != null) {
            // Optimize with Boyer-Moore string search algorithm
            char[] haystack = value.toLowerCase().toCharArray();
            char[] needle = tokens[0];
            if(needle.length == 0) {
                return true;
            }
            for (int i = needle.length - 1, j; i < haystack.length;) {
                for (j = needle.length
                        - 1; needle[j] == haystack[i]; --i, --j) {
                    if(j == 0) {
                        return true;
                    }
                }
                i += Math.max(offsetTable[needle.length - 1 - j],
                        charTable[haystack[i]]);
            }
            return false;
        }
        else {
            return super.in(value);
        }
    }

}
