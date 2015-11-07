/*
 * Copyright (c) 2013-2015 Cinchapi Inc.
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

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.cinchapi.concourse.test.Variables;
import com.cinchapi.concourse.util.Random;
import com.cinchapi.concourse.util.StringSplitter;
import com.google.common.collect.Lists;

/**
 * Unit tests for the {@link StringSplitter} class.
 * 
 * @author Jeff Nelson
 */
public class StringSplitterTest extends ConcourseBaseTest {

    @Test
    public void testStringSplitter() {
        String string = Random.getString();
        char delimiter = string.charAt(Math.abs(Random.getInt()
                % string.length()));
        doTestStringSplitter(string, delimiter);
    }

    @Test
    public void testStringSplitterReproA() {
        doTestStringSplitter("wnwo69", 'w');
    }

    @Test
    public void testStringSplitterReproB() {
        doTestStringSplitter(
                "0n5g6kk2e1wqmwgei4dt b x65 2tglnwrktk8 3xur3rt9i7q z qfbux4ivhpv hpn1om6wmhhvahag5 4xe5rt6oo",
                'o');
    }

    @Test
    public void testStringSplitterReproC() {
        doTestStringSplitter("yj6", 'y');
    }
    
    @Test
    public void testStringSplitterBackToBackDelims(){
        doTestStringSplitter("w  8", ' ');
    }

    /**
     * Execute the logic for the StringSplitter test.
     * 
     * @param string - The string to split
     * @param delimiter - The delimiter to use when splitting
     */
    private void doTestStringSplitter(String string, char delimiter) {
        Variables.register("string", string);
        Variables.register("delimiter", delimiter);
        StringSplitter splitter = new StringSplitter(string, delimiter);
        List<String> actual = Lists.newArrayList();
        while (splitter.hasNext()) {
            actual.add(splitter.next());
        }
        List<String> expected = Lists.newArrayList(string.split(String
                .valueOf(delimiter)));
        Variables.register("expected", expected);
        Variables.register("actual", actual);
        Assert.assertEquals(expected, actual);
    }

}
