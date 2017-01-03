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
package com.cinchapi.concourse.server.plugin.io;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.server.plugin.Packet;
import com.cinchapi.concourse.server.plugin.RemoteMessage;
import com.cinchapi.concourse.server.plugin.data.Dataset;
import com.cinchapi.concourse.server.plugin.data.TObjectResultDataset;
import com.cinchapi.concourse.server.plugin.data.WriteEvent;
import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.thrift.ComplexTObject;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.thrift.TransactionToken;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.Random;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Unit test that the {@link PluginSerializer} can handle all the necessary
 * object types.
 * 
 * @author Jeff Nelson
 */
public class PluginSerializerTest {

    /**
     * A serializer to use within the tests.
     */
    private final PluginSerializer serializer = new PluginSerializer();

    @Test
    public void testSerializeComplexTObject() {
        int count = Random.getScaleCount();
        List<TObject> list = Lists.newArrayListWithCapacity(count);
        for (int i = 0; i < count; ++i) {
            list.add(Convert.javaToThrift(Random.getObject()));
        }
        ComplexTObject expected = ComplexTObject.fromJavaObject(list);
        ByteBuffer buffer = serializer.serialize(expected);
        ComplexTObject actual = serializer.deserialize(buffer);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testSerializePacket() {
        int count = Random.getScaleCount();
        List<WriteEvent> events = Lists.newArrayList();
        for (int i = 0; i < count; ++i) {
            events.add(randomWriteEvent());
        }
        Packet expected = new Packet(events);
        ByteBuffer buffer = serializer.serialize(expected);
        Packet actual = serializer.deserialize(buffer);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testSerializeRemoteMessage() {
        RemoteMessage expected = randomRemoteMessage();
        ByteBuffer buffer = serializer.serialize(expected);
        RemoteMessage actual = serializer.deserialize(buffer);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testSerializeTObject() {
        TObject expected = Convert.javaToThrift(Random.getObject());
        ByteBuffer buffer = serializer.serialize(expected);
        TObject actual = serializer.deserialize(buffer);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testSerializeWriteEvent() {
        WriteEvent expected = randomWriteEvent();
        ByteBuffer buffer = serializer.serialize(expected);
        WriteEvent actual = serializer.deserialize(buffer);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testSerializeJavaSerializable() {
        TestSerializableObject expected = new TestSerializableObject(
                Random.getSimpleString(), Random.getInt());
        ByteBuffer buffer = serializer.serialize(expected);
        TestSerializableObject actual = serializer.deserialize(buffer);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testSerializeDataset() {
        Dataset<Long, String, TObject> expected = new TObjectResultDataset();
        Set<Long> records = Sets.newLinkedHashSet();
        for (int i = 0; i < Random.getScaleCount(); ++i) {
            records.add(Random.getLong());
        }
        Set<String> keys = Sets.newLinkedHashSet();
        for (int i = 0; i < Math.min(Random.getScaleCount(), records.size()); ++i) {
            keys.add(Random.getSimpleString());
        }
        records.forEach((record) -> {
            keys.forEach((key) -> {
                expected.insert(record, key,
                        Convert.javaToThrift(Random.getObject()));
            });
        });
        ByteBuffer buffer = serializer.serialize(expected);
        Dataset<Long, String, TObject> actual = serializer.deserialize(buffer);
        Assert.assertEquals(expected, actual);
    }

    /**
     * Generate a random {@link RemoteMessage} for testing.
     * 
     * @return a RemoteMessage
     */
    private RemoteMessage randomRemoteMessage() {
        int seed = Random.getInt();
        RemoteMessage message;
        if(seed % 3 == 0) {
            message = Reflection
                    .newInstance(
                            "com.cinchapi.concourse.server.plugin.RemoteAttributeExchange",
                            Random.getString(), Random.getString());
        }
        else if(seed % 2 == 0) {
            AccessToken creds = new AccessToken(ByteBuffer.wrap(Random
                    .getString().getBytes(StandardCharsets.UTF_8)));
            TransactionToken transaction = new TransactionToken(creds,
                    Time.now());
            String method = Random.getSimpleString();
            String environment = Random.getSimpleString();
            int argsCount = Math.abs(Random.getInt()) % 8;
            ComplexTObject[] args = new ComplexTObject[argsCount];
            for (int i = 0; i < argsCount; ++i) {
                args[i] = ComplexTObject.fromJavaObject(Random.getObject());
            }
            message = Reflection.newInstance(
                    "com.cinchapi.concourse.server.plugin.RemoteMethodRequest",
                    method, creds, transaction, environment, args);
        }
        else {
            AccessToken creds = new AccessToken(ByteBuffer.wrap(Random
                    .getString().getBytes(StandardCharsets.UTF_8)));
            ComplexTObject response = ComplexTObject.fromJavaObject(Random
                    .getObject());
            message = Reflection
                    .newInstance(
                            "com.cinchapi.concourse.server.plugin.RemoteMethodResponse",
                            creds, response);
        }
        return message;
    }

    /**
     * Generate a random {@link WriteEvent} for testing.
     * 
     * @return a random WriteEvent
     */
    private WriteEvent randomWriteEvent() {
        String key = Random.getSimpleString();
        TObject value = Convert.javaToThrift(Random.getObject());
        long record = Random.getLong();
        WriteEvent.Type type = Time.now() % 2 == 0 ? WriteEvent.Type.ADD
                : WriteEvent.Type.REMOVE;
        long timestamp = Time.now();
        String environment = Random.getSimpleString();
        return new WriteEvent(key, value, record, timestamp, type, environment);
    }

}
