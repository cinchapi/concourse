package com.cinchapi.concourse.plugin.data;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cinchapi.concourse.plugin.data.TrackingLinkedHashMultimap;
import com.cinchapi.concourse.plugin.data.TrackingMultimap;
import com.cinchapi.concourse.plugin.data.TrackingMultimap.VariableType;
import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.cinchapi.concourse.util.Random;

import org.junit.Assert;

/**
 * Tests whether the {@link TrackingMultimap} calculates and returns the correct kind of {@link VariableType}.
 * This is useful for characterizing the data for the visualization engine.
 *
 */
public class TrackingMultimapVariableTypeTest extends ConcourseBaseTest {
    
    private TrackingMultimap<Object, Long> map;
    
    @Before
    public void beforeEachTest() {
        map = TrackingLinkedHashMultimap.create();
    }
    
    @After
    public void afterEachTest() {
        map = null;
    }
    
    @Test
    public void testDichotomous() {
        int count = new java.util.Random().nextInt(1) + 1;  // either a 1 or a 2
        for(int i = 0; i < count; i++) {
            map.put(Random.getObject(), Random.getLong());
        }
        Assert.assertEquals(VariableType.DICHOTOMOUS, map.variableType());
    }
    
    @Test
    public void testNominal() {
        int count = new java.util.Random().nextInt(10) + 3;  // in range (3, 12)
        for(int i = 0; i < count; i++) {
            map.put(Random.getObject(), Random.getLong());
        }
        Assert.assertEquals(VariableType.NOMINAL, map.variableType());
    }
    
    @Test
    public void testInterval() {
        int count = new java.util.Random().nextInt(100000) + 13;  // in range (13, big number)
        for(int i = 0; i < count; i++) {
            map.put(Random.getObject(), Random.getLong());
        }
        Assert.assertEquals(VariableType.INTERVAL, map.variableType());
    }

}
