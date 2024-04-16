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
package com.cinchapi.concourse.server.model;

import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import com.cinchapi.concourse.Tag;
import com.cinchapi.concourse.server.io.ByteableTest;
import com.cinchapi.concourse.test.Variables;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.Numbers;
import com.cinchapi.concourse.util.Random;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Unit tests for {@link com.cinchapi.concourse.server.model.Value}
 *
 * @author Jeff Nelson
 */
@RunWith(Theories.class)
public class ValueTest extends ByteableTest {

    public static @DataPoints Object[] objects = { false, TestData.getDouble(),
            TestData.getFloat(), TestData.getInt(), TestData.getLong(),
            TestData.getString() };

    public static @DataPoints Number[] numbers = { TestData.getDouble(),
            TestData.getFloat(), TestData.getInt(), TestData.getLong() };

    @Test
    @Theory
    public void testCompareTo(Object q1) {
        Object q2 = increase(q1);
        Value v1 = Variables.register("v1",
                Value.wrap(Convert.javaToThrift(q1)));
        Value v2 = Variables.register("v2",
                Value.wrap(Convert.javaToThrift(q2)));
        Assert.assertTrue(v1.compareTo(v2) < 0);
    }

    @Test
    @Theory
    public void testCompareToNegativeInfinity(Object q1) {
        Value v1 = Variables.register("v1",
                Value.wrap(Convert.javaToThrift(q1)));
        Assert.assertTrue(v1.compareTo(Value.NEGATIVE_INFINITY) > 0);
        Assert.assertTrue(Value.NEGATIVE_INFINITY.compareTo(v1) < 0);
        Assert.assertTrue(Value.NEGATIVE_INFINITY
                .compareTo(Value.NEGATIVE_INFINITY) == 0);
        Assert.assertTrue(
                Value.NEGATIVE_INFINITY.compareTo(Value.POSITIVE_INFINITY) < 0);
        Assert.assertTrue(
                Value.POSITIVE_INFINITY.compareTo(Value.NEGATIVE_INFINITY) > 0);
    }

    @Test
    @Theory
    public void testCompareToPositiveInfinity(Object q1) {
        Value v1 = Variables.register("v1",
                Value.wrap(Convert.javaToThrift(q1)));
        Assert.assertTrue(v1.compareTo(Value.POSITIVE_INFINITY) < 0);
        Assert.assertTrue(Value.POSITIVE_INFINITY.compareTo(v1) > 0);
        Assert.assertTrue(Value.POSITIVE_INFINITY
                .compareTo(Value.POSITIVE_INFINITY) == 0);
        Assert.assertTrue(
                Value.POSITIVE_INFINITY.compareTo(Value.NEGATIVE_INFINITY) > 0);
        Assert.assertTrue(
                Value.NEGATIVE_INFINITY.compareTo(Value.POSITIVE_INFINITY) < 0);
    }

    @Test
    @Theory
    public void testCompareToDiffTypes(Number n1, Number n2) {
        if(Numbers.areEqual(n1, n2)) {
            Assert.assertTrue(Value.wrap(Convert.javaToThrift(n1))
                    .compareTo(Value.wrap(Convert.javaToThrift(n2))) == 0);
        }
        else if(Numbers.isGreaterThan(n1, n2)) {
            Assert.assertTrue(Value.wrap(Convert.javaToThrift(n1))
                    .compareTo(Value.wrap(Convert.javaToThrift(n2))) > 0);
        }
        else {
            Assert.assertTrue(Value.wrap(Convert.javaToThrift(n1))
                    .compareTo(Value.wrap(Convert.javaToThrift(n2))) < 0);
        }
    }

    @Test
    public void testCompareReproCON90A() {
        Assert.assertTrue(Value.Sorter.INSTANCE.compare(
                Value.wrap(Convert.javaToThrift(654569943)),
                Value.wrap(Convert.javaToThrift(
                        "2ldexok y9mqipnui o4w85kfa55t9nuzk212kvmf mqvm nr u3412xu6df2gx gsk5 lzv4ssghrbs 3ljiea8 8e2mwauu 12"))) < 0);
    }

    @Test
    public void testCompareReprpCON90() {
        Set<Value> data = Sets.newTreeSet();
        data.add(Value.wrap(Convert.javaToThrift(1734274782)));
        data.add(Value.wrap(Convert.javaToThrift(-703772218)));
        data.add(Value.wrap(Convert.javaToThrift(false)));
        data.add(Value.wrap(Convert.javaToThrift(true)));
        data.add(Value.wrap(Convert.javaToThrift(true)));
        data.add(Value.wrap(Convert.javaToThrift(654569943)));
        data.add(Value.wrap(Convert.javaToThrift(654569943)));
        data.add(Value.wrap(Convert.javaToThrift(717735738)));
        data.add(Value.wrap(Convert.javaToThrift(717735738)));
        data.add(Value.wrap(Convert.javaToThrift(
                "2ldexok y9mqipnui o4w85kfa55t9nuzk212kvmf mqvm nr u3412xu6df2gx gsk5 lzv4ssghrbs 3ljiea8 8e2mwauu 12")));
        data.add(Value.wrap(Convert.javaToThrift(
                "8ol8s8vvekz4awfr6pi84c2jlqzt3uagwtuc4caf0seiqeaapmf0n6z7nw57j4h0ihb9eqxgdeakfr01ige60aca50il8xudogb")));
        data.add(Value.wrap(Convert.javaToThrift(
                "8ol8s8vvekz4awfr6pi84c2jlqzt3uagwtuc4caf0seiqeaapmf0n6z7nw57j4h0ihb9eqxgdeakfr01ige60aca50il8xudogb")));
        data.add(Value.wrap(Convert.javaToThrift(
                "8ol8s8vvekz4awfr6pi84c2jlqzt3uagwtuc4caf0seiqeaapmf0n6z7nw57j4h0ihb9eqxgdeakfr01ige60aca50il8xudogb")));
        data.add(Value.wrap(Convert.javaToThrift(
                "8vsgpp4i4sqo 7wcqxx6342lpai1lypm8icw6yrpkrbwknf51ho1 y5i9d4x")));
        data.add(Value.wrap(Convert.javaToThrift(
                "8y6s9mfwedl21tnk8 ad m  gknrl3 do67lqo1k2yb1soi z  bfhga  k2xu4u rnkui p03ou")));
        data.add(Value.wrap(Convert.javaToThrift(
                "9 6g9swglj86ko96vstgq0bcv ml66ekw1 z7rce zi4wfk")));
        data.add(Value.wrap(Convert.javaToThrift(4324130441596932925L)));
        data.add(Value.wrap(Convert.javaToThrift(0.41255849314087956)));
        data.add(Value.wrap(Convert.javaToThrift(0.5102137787300446)));
        data.add(Value.wrap(Convert.javaToThrift(0.5102137787300446)));
        data.add(Value.wrap(Convert.javaToThrift(0.6456127773032042)));
        data.add(Value.wrap(Convert.javaToThrift(0.7039659687563723)));
        data.add(Value.wrap(Convert.javaToThrift(0.7039659687563723)));
        data.add(Value.wrap(Convert.javaToThrift(0.7039659687563723)));
        data.add(Value.wrap(Convert.javaToThrift(
                "eixhm9et65tb0re4vfnnrjr8d70840hjhr6koau6vfj2qv76vft")));
        data.add(Value.wrap(Convert.javaToThrift(
                "k5 qk0abvcjpgj5qdk byot4n9pc8axs4gf4kacb7baolebri vluvkboq")));
        data.add(Value
                .wrap(Convert.javaToThrift("kcsh84m6w135vagkzydj94j28rr")));
        data.add(Value
                .wrap(Convert.javaToThrift("kcsh84m6w135vagkzydj94j28rr")));
        data.add(Value
                .wrap(Convert.javaToThrift("kcsh84m6w135vagkzydj94j28rr")));
        data.add(Value.wrap(Convert.javaToThrift(
                "nu xgp dz  aln3vk xrezcsv tikkpdrwod 0rp4byh8 ngv8ppvd4j dxkrnfsn0")));
        data.add(Value.wrap(Convert.javaToThrift(
                "nu xgp dz  aln3vk xrezcsv tikkpdrwod 0rp4byh8 ngv8ppvd4j dxkrnfsn0")));

        List<Value> a = Lists.newArrayList(data);
        for (int i = 0; i < a.size(); i++) {
            for (int j = 0; j < a.size(); j++) {
                int cmp = i - j;
                if(cmp == 0) {
                    Assert.assertEquals(0,
                            Value.Sorter.INSTANCE.compare(a.get(i), a.get(j)));
                }
                else if(cmp < 0) {
                    Assert.assertTrue(Value.Sorter.INSTANCE.compare(a.get(i),
                            a.get(j)) < 0);
                }
                else {
                    Assert.assertTrue(Value.Sorter.INSTANCE.compare(a.get(i),
                            a.get(j)) > 0);
                }
            }
        }

    }

    @Test
    public void testCacheServerWrapperIsTamperProof() {
        TObject tobject = TestData.getTObject();
        tobject.cacheServerWrapper("fake");
        Value value = Value.wrap(tobject);
        Assert.assertEquals(value, tobject.getServerWrapper());
    }

    @Test
    public void getCrossTypeCanonicalBytesIntFloat() {
        Value a = Value.wrap(Convert.javaToThrift(18));
        Value b = Value.wrap(Convert.javaToThrift(18.0f));
        Assert.assertEquals(a.getCanonicalBytes(), b.getCanonicalBytes());
        Assert.assertNotEquals(a.getBytes(), b.getBytes());
    }

    @Test
    public void getCrossTypeCanonicalBytesIntDouble() {
        Value a = Value.wrap(Convert.javaToThrift(18));
        Value b = Value.wrap(Convert.javaToThrift(18.0d));
        Assert.assertEquals(a.getCanonicalBytes(), b.getCanonicalBytes());
        Assert.assertNotEquals(a.getBytes(), b.getBytes());
    }

    @Test
    public void getCrossTypeCanonicalBytesLongFloat() {
        Value a = Value.wrap(Convert.javaToThrift(18l));
        Value b = Value.wrap(Convert.javaToThrift(18.0f));
        Assert.assertEquals(a.getCanonicalBytes(), b.getCanonicalBytes());
        Assert.assertNotEquals(a.getBytes(), b.getBytes());
    }

    @Test
    public void getCrossTypeCanonicalBytesLongDouble() {
        Value a = Value.wrap(Convert.javaToThrift(18l));
        Value b = Value.wrap(Convert.javaToThrift(18.0d));
        Assert.assertEquals(a.getCanonicalBytes(), b.getCanonicalBytes());
        Assert.assertNotEquals(a.getBytes(), b.getBytes());
    }

    @Test
    public void getCrossTypeCanonicalBytesIntLong() {
        Value a = Value.wrap(Convert.javaToThrift(18));
        Value b = Value.wrap(Convert.javaToThrift(18l));
        Assert.assertEquals(a.getCanonicalBytes(), b.getCanonicalBytes());
        Assert.assertNotEquals(a.getBytes(), b.getBytes());
    }

    @Test
    public void getCrossTypeCanonicalBytesFloatDouble() {
        Value a = Value.wrap(Convert.javaToThrift(18.0f));
        Value b = Value.wrap(Convert.javaToThrift(18.0d));
        Assert.assertEquals(a.getCanonicalBytes(), b.getCanonicalBytes());
        Assert.assertNotEquals(a.getBytes(), b.getBytes());
    }

    @Test
    public void getCanonicalBytesCornerCaseLongDouble() {
        Value a = Value.wrap(Convert.javaToThrift(Long.MAX_VALUE));
        Value b = Value.wrap(Convert.javaToThrift(Double.MAX_VALUE));
        Assert.assertNotEquals(a.getCanonicalBytes(), b.getCanonicalBytes());
    }

    @Test
    public void getCanonicalBytesCornerCaseIntLong() {
        Value a = Value.wrap(Convert.javaToThrift(Long.MAX_VALUE));
        Value b = Value.wrap(Convert.javaToThrift(Integer.MAX_VALUE + 1));
        Assert.assertNotEquals(a.getCanonicalBytes(), b.getCanonicalBytes());
    }

    @Test
    public void getCanonicalBytesCornerCaseDoubleDouble() {
        Value a = Value.wrap(Convert.javaToThrift(Double.MAX_VALUE));
        Value b = Value.wrap(Convert.javaToThrift(Double.MIN_VALUE));
        Assert.assertNotEquals(a.getCanonicalBytes(), b.getCanonicalBytes());
    }

    @Test
    public void getCanonicalBytesCornerCaseDoubleFloat() {
        Value a = Value.wrap(Convert.javaToThrift(18.123));
        Value b = Value.wrap(Convert.javaToThrift(18.123f));
        Assert.assertEquals(a.getCanonicalBytes(), b.getCanonicalBytes());
    }

    @Test
    public void getCanonicalBytesCornerCaseDoubleLong() {
        double ad = Value.MAX_DOUBLE_REPRESENTED_INTEGER;
        long bd = (long) Value.MAX_DOUBLE_REPRESENTED_INTEGER;
        Value a = Value.wrap(Convert.javaToThrift((ad)));
        Value b = Value.wrap(Convert.javaToThrift(bd));
        Assert.assertEquals(a.getCanonicalBytes(), b.getCanonicalBytes());
    }

    @Test
    public void getCanonicalBytesCornerCaseDoubleLong2() {
        double ad = Value.MAX_DOUBLE_REPRESENTED_INTEGER - 1;
        long bd = (long) Value.MAX_DOUBLE_REPRESENTED_INTEGER - 1;
        Value a = Value.wrap(Convert.javaToThrift((ad)));
        Value b = Value.wrap(Convert.javaToThrift(bd));
        Assert.assertEquals(a.getCanonicalBytes(), b.getCanonicalBytes());
    }

    @Test
    public void getCanonicalBytesCornerCaseDoubleLong3() {
        double ad = Value.MAX_DOUBLE_REPRESENTED_INTEGER + 1;
        long bd = (long) Value.MAX_DOUBLE_REPRESENTED_INTEGER + 1;
        Value a = Value.wrap(Convert.javaToThrift((ad)));
        Value b = Value.wrap(Convert.javaToThrift(bd));
        Assert.assertNotEquals(a.getCanonicalBytes(), b.getCanonicalBytes());
    }

    @Test
    public void getCanonicalBytesCornerCaseDoubleLong4() {
        double ad = Value.MIN_DOUBLE_REPRESENTED_INTEGER;
        long bd = (long) Value.MIN_DOUBLE_REPRESENTED_INTEGER;
        Value a = Value.wrap(Convert.javaToThrift((ad)));
        Value b = Value.wrap(Convert.javaToThrift(bd));
        Assert.assertEquals(a.getCanonicalBytes(), b.getCanonicalBytes());
    }

    @Test
    public void getCanonicalBytesCornerCaseDoubleLong5() {
        double ad = Value.MIN_DOUBLE_REPRESENTED_INTEGER - 1;
        long bd = (long) Value.MIN_DOUBLE_REPRESENTED_INTEGER - 1;
        Value a = Value.wrap(Convert.javaToThrift((ad)));
        Value b = Value.wrap(Convert.javaToThrift(bd));
        Assert.assertNotEquals(a.getCanonicalBytes(), b.getCanonicalBytes());
    }

    @Test
    public void getCanonicalBytesCornerCaseDoubleLong6() {
        double ad = Value.MIN_DOUBLE_REPRESENTED_INTEGER + 1;
        long bd = (long) Value.MIN_DOUBLE_REPRESENTED_INTEGER + 1;
        Value a = Value.wrap(Convert.javaToThrift((ad)));
        Value b = Value.wrap(Convert.javaToThrift(bd));
        Assert.assertEquals(a.getCanonicalBytes(), b.getCanonicalBytes());
    }

    @Test
    public void getCanonicalBytesTagString() {
        String s = Random.getString();
        Value a = Value.wrap(Convert.javaToThrift((Tag.create(s))));
        Value b = Value.wrap(Convert.javaToThrift(s));
        Assert.assertEquals(a.getCanonicalBytes(), b.getCanonicalBytes());
    }

    @Override
    protected Class<Value> getTestClass() {
        return Value.class;
    }

    /**
     * Return a random number of type {@code clazz}.
     * 
     * @param clazz
     * @return a random Number
     */
    private Number getRandomNumber(Class<? extends Number> clazz) {
        if(clazz == Integer.class) {
            return TestData.getInt();
        }
        else if(clazz == Long.class) {
            return TestData.getLong();
        }
        else if(clazz == Double.class) {
            return TestData.getDouble();
        }
        else {
            return TestData.getFloat();
        }
    }

    /**
     * Return an object that is logically greater than {code obj}.
     * 
     * @param obj
     * @return the increased object
     */
    @SuppressWarnings("unchecked")
    private Object increase(Object obj) {
        if(obj instanceof Boolean) {
            return !(boolean) obj;
        }
        else if(obj instanceof String) {
            StringBuilder sb = new StringBuilder();
            for (char c : ((String) obj).toCharArray()) {
                sb.append(++c);
            }
            return sb.toString();
        }
        else {
            Number n = null;
            while (n == null || Numbers.isGreaterThan((Number) obj, n)) {
                n = getRandomNumber((Class<? extends Number>) obj.getClass());
            }
            return n;
        }
    }

}
