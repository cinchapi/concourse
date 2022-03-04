/*
 * Copyright (c) 2013-2022 Cinchapi Inc.
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
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.base.Resources;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.server.plugin.io.PluginSerializer;
import com.cinchapi.concourse.thrift.ComplexTObject;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.FileOps;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;

/**
 * Unit tests for {@link LazyTrackingTObjectResultDataset}
 *
 * @author Jeff Nelson
 */
public class LazyTrackingTObjectResultDatasetTest
        extends TObjectResultDatasetTest {

    @Override
    protected Dataset<Long, String, TObject> createNewDataset() {
        return new LazyTrackingTObjectResultDataset();
    }

    @Test
    public void testPutDoesNotEnableTracking() {
        Assert.assertNull(Reflection.get("tracking", dataset));
        dataset.put(1L,
                ImmutableMap.of("foo", ImmutableSet.of(Convert.javaToThrift(1),
                        Convert.javaToThrift(2), Convert.javaToThrift(3))));
        Assert.assertNull(Reflection.get("tracking", dataset));
    }

    @Test
    public void testInsertDoesEnableTracking() {
        Assert.assertNull(Reflection.get("tracking", dataset));
        dataset.insert(1L, "foo", Convert.javaToThrift(1));
        Assert.assertNotNull(Reflection.get("tracking", dataset));
    }

    @Test
    public void testJitTracking() {
        Assert.assertNull(Reflection.get("tracking", dataset));
        dataset.put(1L,
                ImmutableMap.of("foo", ImmutableSet.of(Convert.javaToThrift(1),
                        Convert.javaToThrift(2), Convert.javaToThrift(3))));
        dataset.put(2L,
                ImmutableMap.of("foo", ImmutableSet.of(Convert.javaToThrift(1),
                        Convert.javaToThrift(2), Convert.javaToThrift(3))));
        dataset.put(3L,
                ImmutableMap.of("foo", ImmutableSet.of(Convert.javaToThrift(1),
                        Convert.javaToThrift(2), Convert.javaToThrift(3))));
        dataset.invert().get("foo");
        Assert.assertNotNull(Reflection.get("tracking", dataset));
    }

    @Test
    public void testTrackingEnabledWhenDeserialized() {
        dataset.put(1L,
                ImmutableMap.of("foo", ImmutableSet.of(Convert.javaToThrift(1),
                        Convert.javaToThrift(2), Convert.javaToThrift(3))));
        dataset.put(2L,
                ImmutableMap.of("foo", ImmutableSet.of(Convert.javaToThrift(1),
                        Convert.javaToThrift(2), Convert.javaToThrift(3))));
        dataset.put(3L,
                ImmutableMap.of("foo", ImmutableSet.of(Convert.javaToThrift(1),
                        Convert.javaToThrift(2), Convert.javaToThrift(3))));
        PluginSerializer serializer = new PluginSerializer();
        ByteBuffer bytes = serializer.serialize(dataset);
        Dataset<Long, String, TObject> dataset2 = serializer.deserialize(bytes);
        Assert.assertNull(Reflection.get("tracking", dataset));
        Assert.assertNotNull(Reflection.get("tracking", dataset2));
    }

    @Test
    public void testSerialization() {
        dataset.insert(1L, "name", Convert.javaToThrift("Jeff Nelson"));
        PluginSerializer serializer = new PluginSerializer();
        ByteBuffer bytes = serializer.serialize(dataset);
        ComplexTObject complex = ComplexTObject.fromJavaObject(bytes);
        ByteBuffer bytes2 = complex.getJavaObject();
        Object obj = serializer.deserialize(bytes2);
        Assert.assertEquals(dataset, obj);
    }

    @Test
    public void testSerializationRepro() {
        String json = FileOps.read(Resources.getAbsolutePath("/data.json"));
        List<Multimap<String, Object>> objects = Convert.anyJsonToJava(json);
        for (Multimap<String, Object> object : objects) {
            long entity = Time.now();
            for (Entry<String, Collection<Object>> entry : object.asMap()
                    .entrySet()) {
                String attribute = entry.getKey();
                for (Object value : entry.getValue()) {
                    dataset.insert(entity, attribute,
                            Convert.javaToThrift(value));
                }
            }
        }
        PluginSerializer serializer = new PluginSerializer();
        ByteBuffer bytes = serializer.serialize(dataset);
        ComplexTObject complex = ComplexTObject.fromJavaObject(bytes);
        ByteBuffer bytes2 = complex.getJavaObject();
        Object obj = serializer.deserialize(bytes2);
        Assert.assertEquals(dataset, obj);
    }

    @Test
    public void testInvertRepro() {
        String json = FileOps.read(Resources.getAbsolutePath("/data.json"));
        List<Multimap<String, Object>> objects = Convert.anyJsonToJava(json);
        for (Multimap<String, Object> object : objects) {
            long entity = Time.now();
            dataset.put(entity,
                    object.asMap().entrySet().stream()
                            .collect(Collectors.toMap(Entry::getKey,
                                    e -> e.getValue().stream()
                                            .map(Convert::javaToThrift)
                                            .collect(Collectors.toSet()))));
        }
        PluginSerializer serializer = new PluginSerializer();
        ByteBuffer bytes = serializer.serialize(dataset);
        ComplexTObject complex = ComplexTObject.fromJavaObject(bytes);
        ByteBuffer bytes2 = complex.getJavaObject();
        dataset = serializer.deserialize(bytes2);
        ObjectResultDataset dataset2 = new ObjectResultDataset(dataset);
        Assert.assertEquals(dataset.toString(), dataset2.toString());

    }

}
