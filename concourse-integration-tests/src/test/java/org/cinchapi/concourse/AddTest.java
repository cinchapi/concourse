package org.cinchapi.concourse;

import org.junit.Assert;
import org.junit.Test;
import org.cinchapi.concourse.util.TestData;

import com.google.common.collect.Iterables;

/**
 * Unit test for API method that adds data to an empty record.
 */
public class AddTest extends ConcourseIntegrationTest {

    @Test
    public void testAdd() {
        long value = TestData.getLong();
        long record = client.add("foo", value);
        Assert.assertEquals(value, client.get("foo", record));
        Assert.assertEquals("foo",
                Iterables.getOnlyElement(client.describe(record)));
    }

}
