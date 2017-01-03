/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse.server.plugin.data;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.server.plugin.data.ObjectResultDataset;
import com.cinchapi.concourse.server.plugin.data.TObjectResultDataset;
import com.cinchapi.concourse.server.plugin.io.PluginSerializer;
import com.cinchapi.concourse.thrift.ComplexTObject;
import com.cinchapi.concourse.util.Convert;

/**
 * Unit tests for {@link ObjectResultDataset}
 * 
 * @author Jeff Nelson
 */
public class ObjectResultDatasetTest {

    @Test
    public void testReproNPE() {
        ObjectResultDataset dataset = new ObjectResultDataset(
                new TObjectResultDataset());
        dataset.insert(0L, "key", true);
        Assert.assertFalse(dataset.get(0L).isEmpty());
        Assert.assertFalse(dataset.get(0L, "key").isEmpty());
    }
    
    @Test
    public void testEntrySetNotEmpty(){
        TObjectResultDataset dataset = new TObjectResultDataset();
        dataset.insert(1L, "age", Convert.javaToThrift(100));
        ObjectResultDataset dataset2 = new ObjectResultDataset(dataset);
        Assert.assertFalse(dataset2.entrySet().isEmpty());
    }
    
    @Test
    public void testConvertToComplexTObject(){
        TObjectResultDataset dataset = new TObjectResultDataset();
        dataset.insert(1L, "age", Convert.javaToThrift(100));
        ObjectResultDataset expected = new ObjectResultDataset(dataset);
        ComplexTObject complex = ComplexTObject.fromJavaObject(expected);
        Map<Long, Map<String, Object>> actual = complex.getJavaObject();
        expected.forEach((key, value) -> {
            Assert.assertTrue(actual.containsKey(key));
            Assert.assertTrue(actual.containsValue(value));
        });
    }
    
    @Test
    public void testSerialization(){
        ObjectResultDataset dataset = new ObjectResultDataset(new TObjectResultDataset());
        dataset.insert(1L, "name", "Jeff Nelson");
        PluginSerializer serializer = new PluginSerializer();
        ByteBuffer bytes = serializer.serialize(dataset);
        ComplexTObject complex = ComplexTObject.fromJavaObject(bytes);
        ByteBuffer bytes2 = complex.getJavaObject();
        Object obj = serializer.deserialize(bytes2);
        Assert.assertEquals(dataset, obj);
    }
    
    @Test
    public void testGetKeyRecordIterator(){
        ObjectResultDataset dataset = new ObjectResultDataset(new TObjectResultDataset());
        dataset.insert(1L, "name", "Jeff Nelson");
        Set<Object> set = dataset.get(1L, "name");
        for (Object obj : set){
            Assert.assertNotNull(obj);
        }
    }
    
    @Test
    public void testInvertedEntrySetWhenNoData(){
        ObjectResultDataset dataset = new ObjectResultDataset();
        Map<String, Map<Object, Set<Long>>> inverted = dataset.invert();
        Assert.assertNotNull(inverted.get("foo").entrySet().iterator());
    }
    
    @Test
    public void testGetEntityGetAttributeIteratorNoNpe(){
        ObjectResultDataset dataset = new ObjectResultDataset();
        dataset.insert(1L, "name", "Jeff Nelson");
        Assert.assertNotNull(dataset.get(1L).get("name").iterator().next());
    }
    
    @Test
    public void testInvertedAttributeKeyDeletedIfEmpty(){
        ObjectResultDataset dataset = new ObjectResultDataset();
        dataset.insert(1L, "name", "Jeff Nelson");
        dataset.remove(1L);
        Assert.assertFalse(dataset.invert().containsKey("name"));
    }
    
    @Test
    public void testInvertedAttributeValueKeyDeletedIfEmpty(){
        ObjectResultDataset dataset = new ObjectResultDataset();
        dataset.insert(1L, "name", "Jeff Nelson");
        dataset.insert(2L, "name", "Jeff Nelson");
        dataset.remove(1L);
        Assert.assertTrue(dataset.invert().containsKey("name"));
        Assert.assertFalse(dataset.invert().get("name").get("Jeff Nelson").contains(1L));
        Assert.assertTrue(dataset.invert().get("name").get("Jeff Nelson").contains(2L));
    }
    
    @Test
    public void testCacheDeletedAfterDeletingEntity(){
        ObjectResultDataset dataset = new ObjectResultDataset();
        dataset.insert(1L, "name", "Jeff Nelson");
        dataset.insert(2L, "name", "Jeff Nelson");
        dataset.remove(1L);
        dataset.forEach((record, data) -> {
            Assert.assertNotEquals(Long.valueOf(1), record);
        });
    }
    
    @Test
    public void testRemoveConcurrentModificationExceptionRepro(){
        ObjectResultDataset dataset = new ObjectResultDataset();
        dataset.insert(1L, "name", "Jeff Nelson");
        dataset.insert(1L, "age", 29);
        dataset.insert(1L, "enabled", true);
        dataset.remove(1L);
    }

}
