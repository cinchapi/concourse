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

import java.io.File;
import java.util.Comparator;

import com.google.common.base.Strings;

/**
 * A {@link Comparator} that sorts files with strictly numerical names between 0
 * and 2^63 - 1 (i.e. a timestamp).
 * 
 * @author Jeff Nelson
 */
public enum NaturalSorter implements Comparator<File> {
    INSTANCE;

    private static AlphaNumericSorter COMPARATOR = new AlphaNumericSorter();

    @Override
    public int compare(File f1, File f2) {
        return COMPARATOR.compare(f1.getName(), f2.getName());
    }

    /**
     * A comparator that sorts string using natural alphanumeric ordering.
     * 
     * @author Jeff Nelson
     */
    private static class AlphaNumericSorter implements Comparator<String> {

        @Override
        public int compare(String a, String b) {
            int aPos = 0;
            int bPos = 0;
            int aPeekPos = 0;
            int bPeekPos = 0;
            for (;;) {
                char aPeek = '\0';
                String aChunk = "";
                // Try to create chunks of all digits
                while (aPos < a.length()) {
                    aPeek = a.charAt(aPos);
                    aPeekPos = aPos;
                    ++aPos;
                    if(Character.isDigit(aPeek)) {
                        aChunk += String.valueOf(aPeek);
                    }
                    else {
                        break;
                    }
                }
                char bPeek = '\0';
                String bChunk = "";
                // Try to create chunks of all digits
                while (bPos < b.length()) {
                    bPeek = b.charAt(bPos);
                    bPeekPos = bPos;
                    ++bPos;
                    if(Character.isDigit(bPeek)) {
                        bChunk += String.valueOf(bPeek);
                    }
                    else {
                        break;
                    }
                }
                if(Strings.isNullOrEmpty(aChunk)
                        || Strings.isNullOrEmpty(bChunk)) {
                    // If either chunk is empty, then it means we aren't doing a
                    // numeric v. numeric comparison, in which case we should
                    // compare the values at which we peeked
                    int comp = aPeek - bPeek;
                    aPos = aPeekPos + 1;
                    bPos = bPeekPos + 1;
                    if(comp != 0 || aPos >= a.length() || bPos >= b.length()) {
                        return comp;
                    }
                    else {
                        continue;
                    }
                }
                else {
                    int aChunkSize = aChunk.length();
                    int bChunkSize = bChunk.length();
                    if(aChunkSize < bChunkSize) {
                        return -1;
                    }
                    else if(bChunkSize < aChunkSize) {
                        return 1;
                    }
                    else {
                        // Chunks are the same size, so comparison is based off
                        // first digit that is different, if any
                        for (int i = 0; i < aChunkSize; ++i) {
                            int comp = aChunk.charAt(i) - bChunk.charAt(i);
                            if(comp != 0) {
                                return comp;
                            }
                        }
                        if(aPos >= a.length() || bPos >= b.length()) {
                            return 0;
                        }
                        else {
                            continue;
                        }
                    }
                }
            }
        }
    }

}
