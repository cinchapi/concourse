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

import org.junit.Ignore;
import org.junit.Test;

import com.cinchapi.concourse.test.ConcourseBenchmarkTest;

/**
 * Unit tests to verify that {@link StringSplitter} is faster than alternative
 * methods.
 * 
 * @author Jeff Nelson
 */
@SuppressWarnings("unused")
public class StringSplitterPerformanceTest extends ConcourseBenchmarkTest {

    @Test
    @Ignore
    public void testSimpleSplit() {
        String string = "The Gangs All Here,www.youtube.com/embed/VlWsLs8G7Kg,,"
                + "\"Anacostia follows the lives of the residents of ANACOSTIA, "
                + "a small residential community in Washington D.C. as they "
                + "navigate through love, betrayal, deception, sex and murder\","
                + "ANACOSTIA,3,7,,Webseries,,,";
        String builtInBenchmark = "builtin";
        String splitterBenchmark = "splitter";
        int rounds = 5000;
        startBenchmark(builtInBenchmark);
        for (int i = 0; i < rounds; ++i) {
            String[] toks = string.split(",");
            for (String tok : toks) {
                continue;
            }
        }
        stopBenchmark(builtInBenchmark);
        startBenchmark(splitterBenchmark);
        for (int i = 0; i < rounds; ++i) {
            StringSplitter it = new StringSplitter(string, ',');
            while (it.hasNext()) {
                it.next();
            }
        }
        stopBenchmark(splitterBenchmark);
        assertFasterThan(splitterBenchmark, builtInBenchmark); // TODO: the
                                                               // built-in split
                                                               // is faster in
                                                               // Java 8...
    }

    @Test
    public void testQuoteAwareSplit() {
        String string = "The Gangs All Here,www.youtube.com/embed/VlWsLs8G7Kg,,"
                + "\"Anacostia follows the lives of the residents of ANACOSTIA, "
                + "a small residential community in Washington D.C. as they "
                + "navigate through love, betrayal, deception, sex and murder\","
                + "ANACOSTIA,3,7,,Webseries,,,";
        String builtInBenchmark = "builtin";
        String splitterBenchmark = "splitter";
        int rounds = 5000;
        startBenchmark(builtInBenchmark);
        for (int i = 0; i < rounds; ++i) {
            String[] toks = Strings.splitStringByDelimiterButRespectQuotes(
                    string, ",");
            for (String tok : toks) {
                continue;
            }
        }
        stopBenchmark(builtInBenchmark);
        startBenchmark(splitterBenchmark);
        for (int i = 0; i < rounds; ++i) {
            StringSplitter it = new QuoteAwareStringSplitter(string, ',');
            while (it.hasNext()) {
                it.next();
            }
        }
        stopBenchmark(splitterBenchmark);
        assertFasterThan(splitterBenchmark, builtInBenchmark);
    }

}
