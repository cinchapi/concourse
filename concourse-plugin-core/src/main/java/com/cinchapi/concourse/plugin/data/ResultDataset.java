/*
 * Copyright (c) 2013-2016 Cinchapi Inc.
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
package com.cinchapi.concourse.plugin.data;

import io.atomix.catalyst.buffer.Buffer;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;

import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.thrift.Type;
import com.google.common.collect.Sets;

/**
 * 
 * 
 * @author Jeff Nelson
 */
public class ResultDataset extends Dataset<Long, String, TObject> {

    @Override
    protected Map<TObject, Set<Long>> createInvertedMultimap() {
        return TrackingLinkedHashMultimap.create();
    }

    @Override
    protected String deserializeAttribute(Buffer buffer) {
        return buffer.readUTF8();
    }

    @Override
    protected Set<Long> deserializeEntities(Buffer buffer) {
        int count = buffer.readInt();
        Set<Long> entities = Sets.newLinkedHashSetWithExpectedSize(count);
        while (entities.size() < count) {
            entities.add(buffer.readLong());
        }
        return entities;
    }

    @Override
    protected TObject deserializeValue(Buffer buffer) {
        Type type = Type.values()[buffer.readByte()];
        int length = buffer.readInt();
        byte[] data = new byte[length];
        buffer.read(data);
        return new TObject(ByteBuffer.wrap(data), type);
    }

    @Override
    protected void serializeAttribute(String attribute, Buffer buffer) {
        buffer.writeUTF8(attribute);
    }

    @Override
    protected void serializeEntities(Set<Long> entities, Buffer buffer) {
        buffer.writeInt(entities.size());
        entities.forEach((entity) -> buffer.writeLong(entity));
    }

    @Override
    protected void serializeValue(TObject value, Buffer buffer) {
        buffer.writeByte(value.getType().ordinal());
        byte[] data = value.getData();
        buffer.writeInt(data.length);
        buffer.write(data);
    }

}
