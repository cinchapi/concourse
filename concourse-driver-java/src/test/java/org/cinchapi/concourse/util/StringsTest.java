/*
 * Copyright (c) 2013-2015 Cinchapi, Inc.
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
package org.cinchapi.concourse.util;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Unit tests for the {@link Strings} utility class.
 * 
 * @author Jeff Nelson
 */
public class StringsTest {

    @Test
    public void testSplitStringByDelimterAndRespectQuotes() {
        String string = "Sachin,,M,\"Maths,Science,English\",Need to improve in these subjects.";
        String[] splitted = Strings.splitStringByDelimiterButRespectQuotes(
                string, ",");
        Assert.assertEquals(
                "[Sachin, , M, \"Maths,Science,English\", Need to improve in these subjects.]",
                Arrays.toString(splitted));
    }

    @Test
    public void testSplitStringByDelimeterWithTrailiningSpaceAndRespectQuotes() {
        String string = "\"New Leaf, Same Page \"";
        String[] splitted = Strings.splitStringByDelimiterButRespectQuotes(
                string, ",");
        Assert.assertEquals("[\"New Leaf, Same Page \"]",
                Arrays.toString(splitted));
    }

    @Test
    public void testSplitWithSingleQuotes() {
        String string = "John said 'hello world'";
        String[] toks = Strings.splitButRespectQuotes(string);
        Assert.assertEquals(
                Lists.newArrayList("John", "said", "\"hello world\""),
                Lists.newArrayList(toks));
    }

}
