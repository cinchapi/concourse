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

import io.atomix.catalyst.buffer.Buffer;

import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.thrift.Type;

/**
 * A {@link ResultDataset} that uses {@link TObject TObjects} for values.
 * 
 * @author Jeff Nelson
 */
public class TObjectResultDataset extends ResultDataset<TObject> {

    /**
     * Return a {@link TObjectResultDataset} that wraps the original
     * {@code dataset}
     * 
     * @param dataset the {@link ObjectResultDataset} to wrap
     * @return the wrapped dataset
     */
    public static TObjectResultDataset wrap(ObjectResultDataset dataset) {
        return (TObjectResultDataset) dataset.thrift;
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
    protected void serializeValue(TObject value, Buffer buffer) {
        buffer.writeByte(value.getType().ordinal());
        byte[] data = value.getData();
        buffer.writeInt(data.length);
        buffer.write(data);
    }

}
