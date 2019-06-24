package com.cinchapi.concourse;

import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.thrift.Operator;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

/**
 *
 */
public class NavigateKeysTest extends ConcourseIntegrationTest {
    /**
     * Setup data for each of the tests.
     *
     * @param client
     */
    private static Map<Long, Map<String, Set<Object>>> setupNavigateEvaluation(
            Concourse client) {
        client.add("name", "john", 1);
        client.add("mother", Link.to(2), 1);

        client.add("name", "leslie", 2);
        client.add("children", 3, 2);

        client.add("name", "doe", 3);
        client.add("mother", Link.to(4), 3);

        client.add("name", "martha", 4);
        client.add("children", 2, 4);

        Map<Long, Map<String, Set<Object>>> expected = Maps.newHashMap();
        Map<String, Set<Object>> row1 = Maps.newHashMap();
        expected.put(1L, row1);
        row1.put("name", Sets.newHashSet("john"));
        row1.put("mother", Sets.newHashSet(Link.to(2)));

        return expected;
    }

    /**
     * Setup data for each of the tests.
     *
     * @param client
     */
    private static Map<Long, Map<String, Set<Object>>> setupNavigateSelectionAndEvaluation(
            Concourse client) {
        client.add("name", "john", 1);
        client.add("mother", Link.to(2), 1);

        client.add("name", "leslie", 2);
        client.add("children", 3, 2);

        client.add("name", "doe", 3);
        client.add("mother", Link.to(4), 3);

        client.add("name", "martha", 4);
        client.add("children", 2, 4);

        Map<Long, Map<String, Set<Object>>> expected = Maps.newHashMap();
        Map<String, Set<Object>> row1 = Maps.newHashMap();
        expected.put(1L, row1);
        row1.put("name", Sets.newHashSet("john"));
        row1.put("mother.children", Sets.newHashSet(3));

        return expected;
    }

    /**
     * Setup data for each of the tests.
     *
     * @param client
     */
    private static Set<Long> setupNavigateFindSelection(
            Concourse client) {
        client.add("name", "john", 1);
        client.add("mother", Link.to(2), 1);

        client.add("name", "leslie", 2);
        client.add("children", 3, 2);

        client.add("name", "doe", 3);
        client.add("mother", Link.to(4), 3);

        client.add("name", "martha", 4);
        client.add("children", 2, 4);

        return Sets.newHashSet(3L);
    }

    @Test
    public void testNavigateAsEvaluationKey() {
        Map<Long, Map<String, Set<Object>>> expected = setupNavigateEvaluation(
                client);
        Map<Long, Map<String, Set<Object>>> actual = client.select("mother.children = 3");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testNavigateFindAsSelectionKey() {
        Set<Long> expected = setupNavigateFindSelection(client);
        Set<Long> actual = client.find("mother.children", Operator.EQUALS, 2);

        Assert.assertEquals(expected, actual);
    }


    @Test
    public void testNavigateAsSelectionKeyAndEvaluationKey() {
        Map<Long, Map<String, Set<Object>>> expected = setupNavigateSelectionAndEvaluation(
                client);
        Map<Long, Map<String, Set<Object>>> actual = client.select(Lists
                .newArrayList("name", "mother.children"), "mother.children = 3");
        Assert.assertEquals(expected, actual);
    }
}
