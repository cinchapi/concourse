package com.cinchapi.concourse;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Link;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.util.Convert;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

/**
 * Unit tests for the {@code jsonify()} API methods. Jsonify takes a list of
 * records and represents each record's data into a JSON formatted string.
 * 
 * @author hyin
 */
public class JsonifyTest extends ConcourseIntegrationTest {

    @Test
    public void testEmptyJsonify() {
        String expected = "[]";
        List<Long> empty = Lists.newArrayList();
        String actual = client.jsonify(empty);
        Assert.assertEquals(expected, actual);
    }

    // Test for corner cases with Concourse data types
    // Double and Link
    @Test
    public void testDoubleAndLinkJsonify() {
        client.add("key", 3.14, 1);
        client.add("key", Link.to(12345), 2);
        Assert.assertTrue(client.jsonify(1).contains("3.14D"));
        Assert.assertTrue(client.jsonify(2).contains("@12345"));
    }

    @Test
    public void testStringToJavaAndBack() {
        String testStr = "{\"key1\": a, \"key2\": b, \"key3\": [c, d, e]}";
        client.insert(testStr, 10L);
        String resultStr = client.jsonify(10L);
        Assert.assertTrue(resultStr.contains("\"key1\":\"a\""));
        Assert.assertTrue(resultStr.contains("\"key2\":\"b\""));
        Assert.assertTrue(resultStr.contains("\"key3\":[\"d\",\"e\",\"c\"]"));
    }

    @Test
    public void testJavaToStringAndBack() {
        Multimap<String, Object> expectedMap = LinkedListMultimap.create();
        expectedMap.put("key1", Arrays.asList(1L, 2L, 3L));
        expectedMap.put("key2", Arrays.asList(4L, 5L, 6L));
        Collection<Long> r1 = Arrays.asList(1L, 2L, 3L);
        Collection<Long> r2 = Arrays.asList(4L, 5L, 6L);
        client.add("key1", r1, 1L);
        client.add("key2", r2, 2L);

        String json = client.jsonify(Arrays.asList(1L, 2L), false);
        List<Multimap<String, Object>> actualMap = Convert.anyJsonToJava(json);

        String expectedkey1 = expectedMap.get("key1").toString();
        String actualkey1 = actualMap.get(0).get("key1").toString();
        String expectedkey2 = expectedMap.get("key2").toString();
        String actualkey2 = actualMap.get(1).get("key2").toString();

        Assert.assertEquals(expectedkey1, actualkey1);
        Assert.assertEquals(expectedkey2, actualkey2);

    }

    @Test
    public void testJsonify() {
        long record1 = 1;
        long record2 = 2;
        long record3 = 3;
        List<Long> recordsList = Lists.newArrayList();
        recordsList.add(record1);
        recordsList.add(record2);
        recordsList.add(record3);
        client.add("a", 1, record1);
        client.add("a", 2, record1);
        client.add("a", 3, record1);
        client.add("b", 1, record1);
        client.add("b", 2, record1);
        client.add("b", 3, record1);
        client.add("c", 1, record2);
        client.add("c", 2, record2);
        client.add("c", 3, record2);
        client.add("d", 1, record3);
        client.add("d", 2, record3);
        client.add("d", 3, record3);
        String json = client.jsonify(recordsList);
        Concourse client2 = Concourse.connect(SERVER_HOST, SERVER_PORT, "admin",
                "admin", Long.toString(System.currentTimeMillis()));
        try {
            Set<Long> created = client2.insert(json);
            List<Map<String, Set<Object>>> expected = Lists
                    .newArrayList(client.select(recordsList).values());
            List<Map<String, Set<Object>>> actual = Lists
                    .newArrayList(client2.select(created).values());
            Assert.assertEquals(expected, actual);
        }
        finally {
            client2.exit();
        }
    }

    @Test
    public void testJsonifyNoPrimaryKey() {
        long record1 = 1;
        long record2 = 2;
        long record3 = 3;
        List<Long> recordsList = Lists.newArrayList();
        recordsList.add(record1);
        recordsList.add(record2);
        recordsList.add(record3);
        client.add("a", 1, record1);
        client.add("a", 2, record1);
        client.add("a", 3, record1);
        client.add("b", 1, record1);
        client.add("b", 2, record1);
        client.add("b", 3, record1);
        client.add("c", 1, record2);
        client.add("c", 2, record2);
        client.add("c", 3, record2);
        client.add("d", 4, record3);
        client.add("d", 5, record3);
        client.add("d", 6, record3);
        String json = client.jsonify(recordsList, false);
        Set<Long> created = client.insert(json);
        List<Map<String, Set<Object>>> expected = Lists
                .newArrayList(client.select(recordsList).values());
        List<Map<String, Set<Object>>> actual = Lists
                .newArrayList(client.select(created).values());
        Assert.assertEquals(expected, actual);
    }
}
