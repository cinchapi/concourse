/*
 * Copyright (c) 2013-2022 Cinchapi Inc.
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
package com.cinchapi.concourse.server.storage.db.search;

import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.util.Random;
import com.cinchapi.concourse.util.TestData;
import com.google.common.base.Strings;

/**
 * Unit tests for {@link LargeTermIndexDeduplicator}.
 *
 * @author Jeff Nelson
 */
public abstract class LargeTermIndexDeduplicatorTest {

    @Test
    public void testSanityCheck() {
        String term = "abrakadabra";
        char[] chars = term.toCharArray();
        Text first = Text.wrap(chars, 1, 4);
        Text second = Text.wrap(chars, 8, 11);
        Assert.assertEquals(first, second);
        LargeTermIndexDeduplicator deduplicator = getDeduplicator(chars);
        Assert.assertTrue(deduplicator.add(first));
        Assert.assertFalse(deduplicator.add(second)); // duplicate
    }

    @Test
    public void testDuplicatesNotAdded() {
        // Demonstrate the adding to a LargeTermIndexDeduplicator has the same
        // effect as adding to a set, for purposes of ensuring no duplicates are
        // permitted.
        String string = Random.getSimpleString();
        Set<String> expected = new HashSet<>();
        while (TestData.getInt() % 3 != 0) {
            string += Random.getSimpleString();
        }
        char[] chars = string.toCharArray();
        LargeTermIndexDeduplicator deduplicator = getDeduplicator(chars);
        for (int i = 0; i < chars.length; ++i) {
            for (int j = i + 1; j <= chars.length; ++j) {
                String ss = string.substring(i, j).trim();
                if(!Strings.isNullOrEmpty(ss)) {
                    Text st = Text.wrap(chars, i, j);
                    Assert.assertEquals(expected.add(ss), deduplicator.add(st));
                }
            }
        }

    }

    /**
     * Return a {@link LageTermIndexDeduplicator} to use in test cases.
     * 
     * @param term
     * @return the deduplicator
     */
    protected abstract LargeTermIndexDeduplicator getDeduplicator(char[] term);

}
