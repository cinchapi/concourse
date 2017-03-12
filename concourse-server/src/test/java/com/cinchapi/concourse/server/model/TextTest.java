/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse.server.model;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.server.io.ByteableTest;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.util.TestData;

/**
 * Unit tests for {@link Text}.
 * 
 * @author Jeff Nelson
 */
public class TextTest extends ByteableTest {

    @Test
    public void testCompareTo() {
        String s1 = TestData.getString();
        String s2 = TestData.getString();
        Text t1 = Text.wrap(s1);
        Text t2 = Text.wrap(s2);
        Assert.assertEquals(s1.compareTo(s2), t1.compareTo(t2));
    }

    @Override
    protected Class<Text> getTestClass() {
        return Text.class;
    }
    
    @Test
    public void testDeserializationWithTrailingWhitespace(){
        String s = "Youtube Embed Link ";
        Text t1 = Text.wrap(s);
        Text t2 = Text.fromByteBuffer(t1.getBytes());
        Assert.assertEquals(t1, t2);
    }

}
