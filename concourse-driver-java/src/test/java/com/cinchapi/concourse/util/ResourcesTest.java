package com.cinchapi.concourse.util;

import java.net.URL;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link Resources} utility methods.
 * 
 * @author raghavbabu
 */
public class ResourcesTest {

    @Test
    public void testGet() {
        URL url = Resources.get("/college.csv");
        Assert.assertEquals(Resources.get("/college.csv"), url);
    }

}
