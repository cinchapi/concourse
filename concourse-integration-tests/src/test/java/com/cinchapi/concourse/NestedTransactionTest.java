package com.cinchapi.concourse;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.util.TestData;

/**
 * Test to verify behavior of nested transactions.
 *
 */
public class NestedTransactionTest extends ConcourseIntegrationTest {

    @Test
    public void testNestedTransaction() {
        int count = TestData.getScaleCount();
        for(int i = 0; i < count; i++) {
            client.stage();
        }
        for(int i = 0; i < count + 1; i++) {
            if(i < count) {
                Assert.assertTrue(client.commit());
            } else {
                Assert.assertFalse(client.commit());
            }
        }
    }

}
