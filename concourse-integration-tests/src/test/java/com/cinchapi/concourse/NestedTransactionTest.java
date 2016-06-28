package com.cinchapi.concourse;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.util.TestData;

public class NestedTransactionTest extends ConcourseIntegrationTest {

    @Test
    public void testNestedTransaction() {
        int count = TestData.getScaleCount();
        for(int i = 0; i < count; i++) {
            client.stage();
        }
        for(int i = 0; i < count; i++) {
            if(i <= count) {
                System.out.println(client.commit());
            } else {
                System.out.println(client.commit());
            }
        }
    }

}
