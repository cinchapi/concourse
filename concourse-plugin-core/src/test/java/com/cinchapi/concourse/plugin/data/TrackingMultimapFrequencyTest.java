package com.cinchapi.concourse.plugin.data;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.plugin.data.TrackingMultimap;
import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.cinchapi.concourse.util.Random;

/**
 * Unit tests for the frequency tracking of the {@link TrackingMultimap}. Makes sure that
 * frequencies are properly updated upon putting or removing from the {@code Map}.
 *
 */
public class TrackingMultimapFrequencyTest extends ConcourseBaseTest {
    
    private TrackingMultimap<String, Integer> map;
    
    @Override
    public void beforeEachTest() {
        map = TrackingLinkedHashMultimap.create();
    }

    @Override
    public void afterEachTest() {
        map = null;
    }
    
    @Test
    public void testFrequencyUpdatedUponPut() {
        int count = Random.getScaleCount();
        String key = Random.getSimpleString();
        int value = Random.getInt();
        for(int i = 1; i <= count; i++) {
            map.put(key, value);
            Assert.assertEquals(Integer.valueOf(i), ((TrackingMultimap<String, Integer>) map).frequency(value));
        }
    }
    
    @Test
    public void testFrequencyUpdatedUponRemove() {
        int count = Random.getScaleCount();
        String key = Random.getSimpleString();
        int value = Random.getInt();
        for(int i = 0; i < count; i++) {
            map.put(key, value);
        }
        map.remove(key);
        Assert.assertEquals(Integer.valueOf(0), ((TrackingMultimap<String, Integer>) map).frequency(value));
    }

}
