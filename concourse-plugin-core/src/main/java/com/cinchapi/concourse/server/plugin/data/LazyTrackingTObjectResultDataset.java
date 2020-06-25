/*
 * Copyright (c) 2013-2020 Cinchapi Inc.
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

import java.util.Map;
import java.util.Set;

import com.cinchapi.concourse.data.sort.Sorter;
import com.cinchapi.concourse.thrift.TObject;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.atomix.catalyst.buffer.Buffer;

/**
 * An analog to {@link TObjectResultDataset} that lazily tracks data in
 * accordance with the specification of {@link LazyTackingResultDataset}.
 *
 * @author Jeff Nelson
 */
public class LazyTrackingTObjectResultDataset extends TObjectResultDataset {

    /**
     * A copy of the data that does not perform tracking.
     */
    protected Map<Long, Map<String, Set<TObject>>> data;

    /**
     * A copy of the data that does perform tracking. This is populated in the
     * {@link #tracking()} method.
     */
    protected TObjectResultDataset tracking = null;

    /**
     * Construct a new instance.
     */
    public LazyTrackingTObjectResultDataset() {
        this.data = Maps.newLinkedHashMap();
    }

    @Override
    public boolean delete(Long entity, String attribute, TObject value) {
        return tracking().delete(entity, attribute, value);
    }

    @Override
    public void deserialize(Buffer buffer) {
        while (buffer.hasRemaining()) {
            long entity = buffer.readLong();
            String attribute = buffer.readUTF8();
            int values = buffer.readInt();
            for (int i = 0; i < values; ++i) {
                TObject value = deserializeValue(buffer);
                insert(entity, attribute, value);
            }
        }
    }

    @Override
    public Set<Entry<Long, Map<String, Set<TObject>>>> entrySet() {
        return data.entrySet();
    }

    @Override
    public boolean equals(Object obj) {
        return data.equals(obj);
    }

    @Override
    public Set<TObject> get(Long entity, String attribute) {
        return data.getOrDefault(entity, ImmutableMap.of()).get(attribute);
    }

    @Override
    public Map<String, Set<TObject>> get(Object entity) {
        return data.get(entity);
    }

    @Override
    public int hashCode() {
        return data.hashCode();
    }

    @Override
    public boolean insert(Long entity, String attribute, TObject value) {
        return tracking().insert(entity, attribute, value);
    }

    @Override
    public Map<String, Map<TObject, Set<Long>>> invert() {
        return tracking().invert();
    }

    @Override
    public Map<TObject, Set<Long>> invert(String attribute) {
        return tracking().invert(attribute);
    }

    @Override
    public Map<String, Set<TObject>> put(Long entity,
            Map<String, Set<TObject>> mappings) {
        return data.put(entity, mappings);
    }

    @Override
    public Map<String, Set<TObject>> remove(Object entity) {
        return data.remove(entity);
    }

    @Override
    public void serialize(Buffer buffer) {
        for (Entry<Long, Map<String, Set<TObject>>> entry : data.entrySet()) {
            buffer.writeLong(entry.getKey());
            for (Entry<String, Set<TObject>> data : entry.getValue()
                    .entrySet()) {
                buffer.writeUTF8(data.getKey());
                buffer.writeInt(data.getValue().size());
                for (TObject value : data.getValue()) {
                    serializeValue(value, buffer);
                }
            }
        }
    }

    @Override
    public void sort(Sorter<Set<TObject>> sorter) {
        data = sorter.sort(data);
        if(tracking != null) {
            tracking();
        }
    }

    @Override
    public void sort(Sorter<Set<TObject>> sorter, long at) {
        data = sorter.sort(data, at);
        if(tracking != null) {
            tracking();
        }
    }

    @Override
    public String toString() {
        return data.toString();
    }

    /**
     * Populate {@link #tracking}.
     */
    protected final void track() {
        tracking = new TObjectResultDataset();
        for (Entry<Long, Map<String, Set<TObject>>> entry : data.entrySet()) {
            long entity = entry.getKey();
            for (Entry<String, Set<TObject>> data : entry.getValue()
                    .entrySet()) {
                String attribute = data.getKey();
                for (TObject value : data.getValue()) {
                    tracking.insert(entity, attribute, value);
                }
            }
        }
        data = tracking;
    }

    /**
     * Populate {@link #tracking} if necessary and return it.
     * 
     * @return the {@link #tracking} copy of the data
     */
    protected final TObjectResultDataset tracking() {
        if(tracking == null) {
            track();
        }
        return tracking;
    }
}
