/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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
package com.cinchapi.concouse.server.upgrade;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.test.UpgradeTest;
import com.cinchapi.concourse.util.ClientServerTests;
import com.google.common.collect.ImmutableMap;

/**
 * Unit test to ensure that search results perform as expected after 0.12 change
 * to preserve stop words in searching and indexing.
 *
 * @author Jeff Nelson
 */
public class Upgrade0_12_0_1SearchTest extends UpgradeTest {

    /**
     * The key under which values are stored.
     */
    private static final String key = "value";

    /**
     * A list of {@link Map Maps} that describe {@link Attribute Attributes}
     * (e.g., string value, search query and expected matching value across
     * versions)
     */
    private static final List<Map<Attribute, Object>> data = new ArrayList<>();
    static {
        // @formatter:off
        data.add(ImmutableMap.of(
                Attribute.HAYSTACK, "complex simplethesimple complex", 
                Attribute.NEEDLE, "complex the complex", 
                Attribute.EXPECTED_0_11, false, 
                Attribute.EXPECTED_0_12, true
        ));
        data.add(ImmutableMap.of(
                Attribute.HAYSTACK, "jeff nelson", 
                Attribute.NEEDLE, "ef lso", 
                Attribute.EXPECTED_0_11, true, 
                Attribute.EXPECTED_0_12, true
        ));
        data.add(ImmutableMap.of(
                Attribute.HAYSTACK, "the quick brown fox jumps over the fence", 
                Attribute.NEEDLE, "th qui brown fo", 
                Attribute.EXPECTED_0_11, false, 
                Attribute.EXPECTED_0_12, true
        ));
        // @formatter:on
    }

    @Test
    public void testSearch() {
        testSearchMatchesInVersion(Attribute.EXPECTED_0_12);
    }

    @Override
    protected String getInitialServerVersion() {
        return "0.11.5";
    }

    @Override
    protected void preUpgradeActions() {
        insertSearchData(); // add matches to db
        ClientServerTests.insertRandomDataInStorageFormatV3(server, "default");
        insertSearchData(); // add matches to buffer
        testSearchMatchesInVersion(Attribute.EXPECTED_0_11);
    }

    /**
     * Insert all the search data using the current client.
     */
    private void insertSearchData() {
        for (Map<Attribute, Object> row : data) {
            String haystack = (String) row.get(Attribute.HAYSTACK);
            client.add(key, haystack);
        }
    }

    /**
     * Test all the search matches in the client's current version and
     * anticipate matching based on the provided {@code attribute}
     * 
     * @param attribute
     */
    private void testSearchMatchesInVersion(Attribute attribute) {
        for (Map<Attribute, Object> row : data) {
            String needle = (String) row.get(Attribute.NEEDLE);
            boolean expected = (boolean) row.get(attribute);
            Set<Long> results = client.search(key, needle);
            if(expected) {
                Assert.assertEquals(2, results.size());
            }
            else {
                Assert.assertTrue(results.size() < 2);
            }
        }
    }

    static enum Attribute {
        HAYSTACK, NEEDLE, EXPECTED_0_11, EXPECTED_0_12
    }

}
