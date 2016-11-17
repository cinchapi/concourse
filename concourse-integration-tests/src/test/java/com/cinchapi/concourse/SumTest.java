package com.cinchapi.concourse;


import java.math.BigDecimal;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.beust.jcommander.internal.Lists;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.thrift.Operator;


/**
 * Tests to check the functionality of sum feature.
 * 
 * @author Raghav Babu
 */
public class SumTest extends ConcourseIntegrationTest {

    @Test
    public void testSumKeyCcl() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add("name", "bar", 2);
        client.add(key, 19, 2);
        client.add("name", "bar", 2);
        client.add(key, 15, 2);
        BigDecimal actual = new BigDecimal(34);
        BigDecimal expected = client.sum(key, "name = bar");
        Assert.assertEquals(expected.intValue(), actual.intValue());
    }

    @Test
    public void testSumKeyRecordTime() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add(key, 19, 1);
        BigDecimal actual = new BigDecimal(49);
        BigDecimal expected = client.sum(key, 1, Timestamp.now());
        Assert.assertEquals(expected.intValue(), actual.intValue());
    }
    
    @Test
    public void testSumKeyCclTime() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add("name", "bar", 2);
        client.add(key, 19, 2);
        client.add("name", "bar", 2);
        client.add(key, 15, 2);
        BigDecimal actual = new BigDecimal(34);
        BigDecimal expected = client.sum(key, "name = bar", Timestamp.now());
        Assert.assertEquals(expected.intValue(), actual.intValue());
    }
    
    @Test
    public void testSumKeyCriteria() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add("name", "bar", 2);
        client.add(key, 19, 2);
        client.add("name", "bar", 2);
        client.add(key, 15, 2);
        BigDecimal actual = new BigDecimal(34);
        BigDecimal expected = client.sum(key, Criteria.where().key("age")
                .operator(Operator.LESS_THAN).value(20).build());
        Assert.assertEquals(expected.intValue(), actual.intValue());
    }
    
    @Test
    public void testSumKeyCriteriaTime() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add("name", "bar", 2);
        client.add(key, 19, 2);
        client.add("name", "bar", 2);
        client.add(key, 15, 2);
        BigDecimal actual = new BigDecimal(34);
        BigDecimal expected = client.sum(key, Criteria.where().key("age")
                .operator(Operator.LESS_THAN).value(20).build(), Timestamp.now());
        Assert.assertEquals(expected.intValue(), actual.intValue());
    }
   
    @Test
    public void testSumKeyRecord() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add(key, 19, 1);
        BigDecimal actual = new BigDecimal(49);
        BigDecimal expected = client.sum(key, 1);
        Assert.assertEquals(expected.intValue(), actual.intValue());
    }
    
    @Test(expected = RuntimeException.class)
    public void testSumKeyRecordException() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add(key, "fifteen", 1);
        client.sum(key, 1);
    }
    
    @Test
    public void testSumKey() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add("name", "bar", 2);
        client.add(key, 19, 2);
        client.add("name", "bar", 2);
        client.add(key, 15, 2);
        BigDecimal actual = new BigDecimal(64);
        BigDecimal expected = client.sum(key);
        Assert.assertEquals(expected.intValue(), actual.intValue());
    }
    
    @Test(expected = RuntimeException.class)
    public void testSumKeyException() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add(key, "fifteen", 1);
        client.add("name", "bar", 2);
        client.add(key, 15, 2);
        client.sum(key);
    }
    
    @Test
    public void testSumKeyTime() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add("name", "bar", 2);
        client.add(key, 19, 2);
        client.add("name", "bar", 2);
        client.add(key, 15, 2);
        BigDecimal actual = new BigDecimal(64);
        BigDecimal expected = client.sum(key, Timestamp.now());
        Assert.assertEquals(expected.intValue(), actual.intValue());
    }
    
    @Test(expected = RuntimeException.class)
    public void testSumKeyTimeException() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add(key, "fifteen", 1);
        client.add("name", "bar", 2);
        client.add(key, 15, 2);
        client.sum(key, Timestamp.now());
    }
    
    @Test
    public void testSumKeyRecords() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add("name", "bar", 2);
        client.add(key, 20, 2);
        BigDecimal actual = new BigDecimal(50);
        List<Long> list = Lists.newArrayList();
        list.add((long) 1);
        list.add((long) 2);
        BigDecimal expected = client.sum(key,list);
        Assert.assertEquals(expected.intValue(), actual.intValue());
    }
    
    @Test(expected = RuntimeException.class)
    public void testSumKeyRecordsException() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add("name", "bar", 2);
        client.add(key, "fifty", 2);
        List<Long> list = Lists.newArrayList();
        list.add((long) 1);
        list.add((long) 2);
        client.sum(key,list);
    }
    
    @Test
    public void testSumKeyRecordsTime() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add("name", "bar", 2);
        client.add(key, 20, 2);
        BigDecimal actual = new BigDecimal(50);
        List<Long> list = Lists.newArrayList();
        list.add((long) 1);
        list.add((long) 2);
        BigDecimal expected = client.sum(key,list, Timestamp.now());
        Assert.assertEquals(expected.intValue(), actual.intValue());
    }
    
    public void testSumKeyRecordsTimeException() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add("name", "bar", 2);
        client.add(key, "fifty", 2);
        List<Long> list = Lists.newArrayList();
        list.add((long) 1);
        list.add((long) 2);
        client.sum(key,list, Timestamp.now());
    }
}
