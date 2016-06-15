package com.cinchapi.concourse.bugrepro;

import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.google.common.collect.Sets;

/**
 * Unit test that attempts to reproduce the issue described in CON-469.
 * 
 * @author Raghav Babu
 */
public class CON469 extends ConcourseIntegrationTest {

    @Test
    public void reproGetInventory() {
        client.stage();
        client.add("name", "Sachin", 1);
        Set<Long> expected = Sets.newHashSet();
        expected.addAll(client.find("name", "Sachin"));
        Assert.assertEquals(expected, client.inventory());
        client.abort();
        Assert.assertNotEquals(expected, client.inventory());
    }
}
