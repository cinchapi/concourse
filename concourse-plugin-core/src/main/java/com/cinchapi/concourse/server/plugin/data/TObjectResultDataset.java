/*
 * Copyright (c) 2013-2019 Cinchapi Inc.
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
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.cinchapi.concourse.data.sort.SortableTableMap;
import com.cinchapi.concourse.data.sort.Sorter;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.thrift.Type;
import io.atomix.catalyst.buffer.Buffer;

/**
 * A {@link ResultDataset} that uses {@link TObject TObjects} for values.
 * 
 * @author Jeff Nelson
 */
public class TObjectResultDataset extends ResultDataset<TObject>
        implements SortableTableMap<Set<TObject>> {

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

    /**
     * The {@link Sorter} that has been provided by the {@link #sort(Sorter)}
     * method. The {@code sorter} is applied on the fly when a call is made to
     * {@link #entrySet()}.
     */
    @Nullable
    private Sorter<Set<TObject>> sorter;

    @Override
    public Set<Entry<Long, Map<String, Set<TObject>>>> entrySet() {
        Set<Entry<Long, Map<String, Set<TObject>>>> entrySet = super.entrySet();
        if(sorter != null) {
            // Sort the #entrySet on the fly so that iteration (and all
            // derivative functionality) adheres to the order specified by the
            // {@link #sort()}.
            Map<Long, Map<String, Set<TObject>>> map = entrySet.stream()
                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
            map = sorter.sort(map);
            entrySet = map.entrySet();
        }
        return entrySet;
    }

    @Override
    public void sort(Sorter<Set<TObject>> sorter) {
        this.sorter = sorter;
    }

    @Override
    protected Map<TObject, Set<Long>> createInvertedMultimap() {
        return TrackingLinkedHashMultimap.create(TObject.comparator());
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
