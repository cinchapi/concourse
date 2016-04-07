/*
 * Copyright (c) 2013-2016 Cinchapi Inc.
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
package com.cinchapi.concourse.lang;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.lang.NaturalLanguage;
import com.cinchapi.concourse.test.ConcourseBaseTest;

/**
 * Unit tests for the {@link NaturalLanguage} utils.
 * 
 * @author Jeff Nelson
 */
public class NaturalLanguageTest extends ConcourseBaseTest {

    @Test
    public void testParseTimeStringInDefaultFormat() {
        String str = "Sat Mar 07, 2015 @ 8:11:35:36 PM EST";
        long expected = Timestamp.parse(str, Timestamp.DEFAULT_FORMATTER).getMicros();
        Assert.assertEquals(expected, NaturalLanguage.parseMicros(str));
    }

}
