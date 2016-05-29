package com.cinchapi.concourse;

import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.Iterables;

public class RecallTest extends ConcourseIntegrationTest {
    @Test
    public void testOneRevision() {
        String key = "foo";
        String value = TestData.getText().toString();
        
        long record = client.add(key, value);
        
        String recalledValue = Iterables.getOnlyElement(
                client.recall(record, 0).get(key)
        ).toString();
        
        Assert.assertTrue(recalledValue.equals(value));
    }
    
    @Test
    public void testOneKeyRevision() {
        String key = "foo";
        String value = TestData.getText().toString();
        
        long record = client.add(key, value);
        
        String recalledValue = Iterables.getOnlyElement(
                client.recall(record, key, 0)
        ).toString();
        
        Assert.assertTrue(recalledValue.equals(value));
    }
    
    @Test
    public void testRecallAfterSet() {
        String key = "foo";
        String value = TestData.getText().toString();
        String value2 = TestData.getText().toString();
        
        long record = client.add(key, value);
        
        client.set(key,  value2, record);
        
        Object initialValue = Iterables.getOnlyElement(
                client.recall(record, 2).get(key)
        ).toString();
        
        Object nullValue = client.recall(record, 1).get(key);
        
        String setValue = Iterables.getOnlyElement(
                client.recall(record, 0).get(key)
        ).toString();
        
        Assert.assertTrue(nullValue == null);
        
        Assert.assertTrue(initialValue.equals(value));
        
        Assert.assertTrue(setValue.equals(value2));
    }
    
    @Test
    public void testRecallKeyAfterSet() {
        String key = "foo";
        String value = TestData.getText().toString();
        String value2 = TestData.getText().toString();
        
        long record = client.add(key, value);
        
        client.set(key,  value2, record);
        
        Object initialValue = Iterables.getOnlyElement(
                client.recall(record, key, 2)
        ).toString();
        
        Object nullValue = client.recall(record, 1).get(key);
        
        String setValue = Iterables.getOnlyElement(
                client.recall(record, key, 0)
        ).toString();
        
        Assert.assertTrue(nullValue == null);
        
        Assert.assertTrue(initialValue.equals(value));
        
        Assert.assertTrue(setValue.equals(value2));
    }
    
    @Test
    public void testRecallAfterAdd() {
        String key = "foo";
        String value = TestData.getText().toString();
        
        long record = client.add(key, value);
        client.add(key, value, record);
        
        Map<String, Set<Object>> rawRecord = client.recall(record, 0);
        
        int numberOfElements = rawRecord.get(key).size();
        
        Assert.assertEquals(numberOfElements, 1);
    }
    
    @Test
    public void testRecallKeyAfterAdd() {
        String key = "foo";
        String value = TestData.getText().toString();
        
        long record = client.add(key, value);
        client.add(key, value, record);
        
        Set<Object> rawRecord = client.recall(record, key, 0);
        
        int numberOfElements = rawRecord.size();
        
        Assert.assertEquals(numberOfElements, 1);
    }
}