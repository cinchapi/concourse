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
package com.cinchapi.concourse.thrift;

import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import com.cinchapi.common.base.Array;
import com.cinchapi.concourse.Link;
import com.cinchapi.concourse.Tag;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.Random;

/**
 * Unit tests for the constraints promised by the
 * {@link com.cinchapi.concourse.thrift.TObject}
 * class.
 *
 * @author Jeff Nelson
 */
@RunWith(Theories.class)
public class TObjectTest {

    @DataPoints
    public static Operator[] operators = Operator.values();

    @Test
    public void testCompareTo() {
        TObject big = Convert.javaToThrift(1564359884275001L);
        TObject small = Convert.javaToThrift(1564359884211000L);
        Assert.assertTrue(big.compareTo(small) > 0);
        Assert.assertTrue(small.compareTo(big) < 0);
    }

    @Test
    public void testCompareToIgnoreCase() {
        TObject a = Convert.javaToThrift("a");
        TObject b = Convert.javaToThrift("B");
        TObject c = Convert.javaToThrift("c");
        Assert.assertTrue(a.compareToIgnoreCase(b) < 0);
        Assert.assertTrue(a.compareToIgnoreCase(c) < 0);
        Assert.assertTrue(b.compareToIgnoreCase(c) < 0);
        Assert.assertTrue(b.compareToIgnoreCase(a) > 0);
        TObject a2 = Convert.javaToThrift("A");
        Assert.assertEquals(0, a.compareToIgnoreCase(a2));
    }

    @Test
    public void testCompareToVsEqualsConsistency() {
        TObject a1 = Convert.javaToThrift("a");
        TObject a2 = Convert.javaToThrift("A");
        Assert.assertNotEquals(a1, a2);
        Assert.assertNotEquals(0, a1.compareTo(a2));
    }

    @Test
    public void testIsBlankNegative() {
        int count = Random.getScaleCount();
        String s = "";
        for (int i = 0; i < count; ++i) {
            s += " ";
        }
        String extra = "";
        while (StringUtils.isBlank(extra)) {
            extra = Random.getString();
        }
        s += extra;
        count = Random.getScaleCount();
        for (int i = 0; i < count; ++i) {
            s += " ";
        }
        TObject t = Convert.javaToThrift(s);
        Assert.assertFalse(t.isBlank());
    }

    @Test
    public void testIsBlankPositive() {
        int count = Random.getScaleCount();
        String s = "";
        for (int i = 0; i < count; ++i) {
            s += " ";
        }
        TObject t = Convert.javaToThrift(s);
        Assert.assertTrue(t.isBlank());
    }

    @Test
    public void testIsLinksTo() {
        TObject a = Convert.javaToThrift(Link.to(1));
        Assert.assertTrue(a.is(Operator.LINKS_TO, Convert.javaToThrift(1)));
    }

    @Test
    public void testNormalizeLinksToNotLong() {
        TObject value = Convert.javaToThrift(Random.getString());
        Assert.assertEquals(value,
                TObject.alias(Operator.LINKS_TO, value).values()[0]);
    }

    @Test
    @Theory
    public void testNormalizeOperator(Operator operator) {
        Operator expected = null;
        switch (operator) {
        case LIKE:
            expected = Operator.REGEX;
            break;
        case NOT_LIKE:
            expected = Operator.NOT_REGEX;
            break;
        case LINKS_TO:
            expected = Operator.EQUALS;
            break;
        default:
            expected = operator;
            break;
        }
        Assert.assertEquals(expected,
                TObject.alias(operator, Array.containing()).operator());
    }

    @Test
    @Theory
    public void testNormalizeValue(Operator operator) {
        long num = Random.getLong();
        Object value = null;
        Object expected = null;
        switch (operator) {
        case REGEX:
        case NOT_REGEX:
            value = putNumberWithinPercentSign(num);
            expected = putNumberWithinStarSign(num);
            break;
        case LINKS_TO:
            value = num;
            expected = Link.to(num);
            break;
        default:
            value = num;
            expected = num;
            break;
        }
        Assert.assertEquals(Convert.javaToThrift(expected), TObject
                .alias(operator, Convert.javaToThrift(value)).values()[0]);
    }

    @Test
    public void testNullIsAlwaysBlank() {
        Assert.assertTrue(TObject.NULL.isBlank());
    }

    @Test
    public void testTagAndStringAreEqual() {
        String base = Random.getString();
        TObject string = Convert.javaToThrift(base);
        TObject tag = Convert.javaToThrift(Tag.create(base));
        Assert.assertEquals(tag, string);
    }

    @Test
    public void testTagAndStringDoNotMatch() {
        String base = Random.getString();
        TObject string = Convert.javaToThrift(base);
        TObject tag = Convert.javaToThrift(Tag.create(base));
        Assert.assertFalse(tag.matches(string));
        Assert.assertFalse(string.matches(tag));
    }

    @Test
    public void testTagAndStringHaveSameHashCode() {
        String base = Random.getString();
        TObject string = Convert.javaToThrift(base);
        TObject tag = Convert.javaToThrift(Tag.create(base));
        Assert.assertEquals(tag.hashCode(), string.hashCode());
    }

    /**
     * This method will convert {@link long} into String. It will put %
     * (percent) Sign at the both
     * end and \\% in the middle of {@link String}.
     * 
     * @param num
     * @return {@link String}
     */
    private String putNumberWithinPercentSign(long num) {
        String str = String.valueOf(num);
        return "%" + str.substring(0, str.length() / 2) + "\\%"
                + str.substring(str.length() / 2, str.length()) + "%";
    }

    /**
     * This method will convert {@link long} into {@link String}. It will put *
     * (percent) sign at the both
     * end and % in the middle of {@link String}.
     * 
     * @param num
     * @return {@link String}
     */
    private String putNumberWithinStarSign(long num) {
        String str = String.valueOf(num);
        return ".*" + str.substring(0, str.length() / 2) + "%"
                + str.substring(str.length() / 2, str.length()) + ".*";
    }

}
