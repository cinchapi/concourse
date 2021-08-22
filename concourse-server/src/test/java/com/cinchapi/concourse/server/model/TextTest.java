/*
 * Copyright (c) 2013-2019 Cinchapi Inc.
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
package com.cinchapi.concourse.server.model;

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.server.io.ByteableTest;
import com.cinchapi.concourse.server.io.Composite;
import com.cinchapi.concourse.util.Random;
import com.cinchapi.concourse.util.TestData;

/**
 * Unit tests for {@link com.cinchapi.concourse.server.model.Text}.
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
    public void testDeserializationWithTrailingWhitespace() {
        String s = "Youtube Embed Link ";
        Text t1 = Text.wrap(s);
        Text t2 = Text.fromByteBuffer(t1.getBytes());
        Assert.assertEquals(t1, t2);
    }

    @Test
    public void testCharTextStringTextToString() {
        String str = "Jeff Nelson";
        int start = 2;
        int end = 6;
        Text t1 = Text.wrap(str.substring(start, end));
        Text t2 = Text.wrap(str.toCharArray(), start, end);
        Assert.assertEquals(t1.toString(), t2.toString());
    }

    @Test
    public void testStringTextTrimEmpty() {
        String str = " ";
        Text txt = Text.wrap(str);
        Assert.assertEquals(str.trim(), txt.trim().toString());
    }

    @Test
    public void testCharTextTrimEmpty() {
        String str = " ";
        Text txt = Text.wrap(str.toCharArray(), 0, str.length());
        Assert.assertEquals(str.trim(), txt.trim().toString());
    }

    @Test
    public void testStringTextCharTextEmptyEquals() {
        Assert.assertEquals(Text.wrap(""), Text.wrap("".toCharArray(), 0, 0));
    }

    @Test
    public void testStringTextTrim() {
        String str = " jeff nel s o n   ";
        Text txt = Text.wrap(str);
        Assert.assertEquals(str.trim(), txt.trim().toString());
    }

    @Test
    public void testChartTextTrim() {
        String str = " jeff nel s o n   ";
        Text txt = Text.wrap(str.toCharArray(), 0, str.length());
        Assert.assertEquals(str.trim(), txt.trim().toString());
    }

    @Test
    public void testStringTextCharTextEquals() {
        String str = Random.getString();
        Assert.assertEquals(Text.wrap(str),
                Text.wrap(str.toCharArray(), 0, str.length()));
    }

    @Test
    public void testStringTextCharTextSubSequenceEquals() {
        String str = Random.getString();
        int start = Math.abs(Random.getInt()) % str.length();
        int end;
        if(start < str.length() - 1) {
            end = start + (Math.abs(Random.getInt()) % (str.length() - start));
        }
        else {
            end = str.length();
        }
        Assert.assertEquals(Text.wrap(str.substring(start, end)),
                Text.wrap(str.toCharArray(), start, end));
    }

    @Test
    public void testStringTextCharTextHashCode() {
        String str = Random.getString();
        int start = Math.abs(Random.getInt()) % str.length();
        int end;
        if(start < str.length() - 1) {
            end = start + (Math.abs(Random.getInt()) % (str.length() - start));
        }
        else {
            end = str.length();
        }
        Assert.assertEquals(Text.wrap(str.substring(start, end)).hashCode(),
                Text.wrap(str.toCharArray(), start, end).hashCode());
    }

    @Test
    public void testStringTextCharTextToString() {
        String str = Random.getString();
        int start = Math.abs(Random.getInt()) % str.length();
        int end;
        if(start < str.length() - 1) {
            end = start + (Math.abs(Random.getInt()) % (str.length() - start));
        }
        else {
            end = str.length();
        }
        Assert.assertEquals(Text.wrap(str.substring(start, end)).toString(),
                Text.wrap(str.toCharArray(), start, end).toString());
    }

    @Test
    public void testStringTextCharTextGetBytes() {
        String str = Random.getString();
        int start = Math.abs(Random.getInt()) % str.length();
        int end;
        if(start < str.length() - 1) {
            end = start + (Math.abs(Random.getInt()) % (str.length() - start));
        }
        else {
            end = str.length();
        }
        Assert.assertEquals(Text.wrap(str.substring(start, end)).getBytes(),
                Text.wrap(str.toCharArray(), start, end).getBytes());
    }

    @Test
    public void testStringTextCompareTo() {
        String str1 = Random.getString();
        int start1 = Math.abs(Random.getInt()) % str1.length();
        int end1;
        if(start1 < str1.length() - 1) {
            end1 = start1
                    + (Math.abs(Random.getInt()) % (str1.length() - start1));
        }
        else {
            end1 = str1.length();
        }
        String str2 = Random.getString();
        int start2 = Math.abs(Random.getInt()) % str2.length();
        int end2;
        if(start2 < str2.length() - 1) {
            end2 = start2
                    + (Math.abs(Random.getInt()) % (str2.length() - start2));
        }
        else {
            end2 = str2.length();
        }
        Assert.assertEquals(
                str1.substring(start1, end1)
                        .compareTo(str2.substring(start2, end2)),
                Text.wrap(str1.toCharArray(), start1, end1).compareTo(
                        Text.wrap(str2.toCharArray(), start2, end2)));
    }

    @Test
    public void testStringTextCharTextCompareTo() {
        String str1 = Random.getString();
        String str2 = Random.getString();
        Text txt1 = Text.wrap(str1);
        Text txt2 = Text.wrap(str2.toCharArray(), 0, str2.length());
        Assert.assertEquals(txt1.compareTo(txt2), str1.compareTo(str2));
        Assert.assertEquals(txt2.compareTo(txt1), str2.compareTo(str1));
    }

    @Test
    public void testFromByteBufferCached() {
        String string = Random.getString();
        Text text = Text.wrap(string);
        ByteBuffer bytes = text.getBytes();
        Text t1 = Text.fromByteBufferCached(bytes);
        Text t2 = null;
        Text t3 = null;
        while (t2 != t1 || t3 != t2) {
            // Wait for lazy cache to kick in...
            t1 = Text.fromByteBufferCached(bytes);
            t2 = Text.fromByteBufferCached(bytes);
            t3 = Text.wrapCached(string);
        }
        Assert.assertSame(t1, t2);
        Assert.assertEquals(t2, text);
        Assert.assertSame(t2, t3);
    }

    @Test
    public void testCanonicalLengthConsistencyStringTextCharText() {
        String str = "&nbsp;</p><p><s";
        Text t1 = Text.wrap(str);
        Text t2 = Text.wrap(str.toCharArray(), 0, str.length());
        Assert.assertEquals(t1.getCanonicalLength(), t2.getCanonicalLength());
    }

    @Test
    public void testCompositeConsistency() {
        String key = "description";
        String term = "&nbsp;</p><p><s";
        Composite c1 = Composite.create(Text.wrap(key), Text.wrap(term));
        int count = TestData.getScaleCount();
        for (int i = 0; i < count; ++i) {
            Composite c2 = Composite.create(Text.wrap(key),
                    Text.wrap(term.toCharArray(), 0, term.length()));
            Assert.assertEquals(c1, c2);
        }
    }

    @Test
    public void testFromByteBufferSizeStringText() {
        Text text = Text.wrap(Random.getString());
        ByteBuffer bytes = text.getBytes();
        int expected = text.size();
        text = Text.fromByteBuffer(bytes);
        int actual = text.size();
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testFromByteBufferSizeChartText() {
        String string = Random.getString();
        Text text = Text.wrap(string.toCharArray(), 0, string.length());
        ByteBuffer bytes = text.getBytes();
        int expected = text.size();
        text = Text.fromByteBuffer(bytes);
        int actual = text.size();
        Assert.assertEquals(expected, actual);
    }

}
