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
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.cinchapi.common.base.AdHocIterator;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.thrift.Type;
import com.cinchapi.concourse.util.Convert;

/**
 * A {@link ResultDataset} that wraps a {@link TObjectDataset} and lazily
 * transforms values.
 * 
 * @author Jeff Nelson
 */
public class ObjectResultDataset extends ResultDataset<Object> {

    /**
     * The internal dataset that contains the data.
     */
    private Dataset<Long, String, TObject> data;

    /**
     * Construct a new instance.
     * 
     * @param data
     */
    public ObjectResultDataset(Dataset<Long, String, TObject> data) {
        this.data = data;
    }

    @Override
    public boolean delete(Long entity, String attribute, Object value) {
        return data.delete(entity, attribute, Convert.javaToThrift(value));
    }

    @Override
    public Set<Object> get(Long entity, String attribute) {
        return new LazyTransformSet(entity, attribute);
    }

    @Override
    public Map<String, Set<Object>> get(Object entity) {
        if(entity instanceof Long) {
            return new LazyTransformMap((long) entity);
        }
        else {
            return null;
        }
    }

    @Override
    public boolean insert(Long entity, String attribute, Object value) {
        return data.insert(entity, attribute, Convert.javaToThrift(value));
    }

    @Override
    protected Map<Object, Set<Long>> createInvertedMultimap() {
        return TrackingLinkedHashMultimap.create();
    }

    @Override
    protected Object deserializeValue(Buffer buffer) {
        Type type = Type.values()[buffer.readByte()];
        int length = buffer.readInt();
        byte[] data = new byte[length];
        buffer.read(data);
        TObject value = new TObject(ByteBuffer.wrap(data), type);
        return Convert.thriftToJava(value);
    }

    @Override
    protected void serializeValue(Object value, Buffer buffer) {
        TObject value0 = Convert.javaToThrift(value);
        buffer.writeByte(value0.getType().ordinal());
        byte[] data = value0.getData();
        buffer.writeInt(data.length);
        buffer.write(data);
    }

    /**
     * A wrapper map that transforms written values from Object to TObject and
     * read values from TObject to Object on-demand.
     * 
     * @author Jeff Nelson
     */
    private class LazyTransformMap extends AbstractMap<String, Set<Object>> {

        private final long entity;

        /**
         * Construct a new instance.
         * 
         * @param entity
         */
        private LazyTransformMap(long entity) {
            this.entity = entity;
        }

        @Override
        public Set<Entry<String, Set<Object>>> entrySet() {
            return new AbstractSet<Entry<String, Set<Object>>>() {

                @Override
                public Iterator<Entry<String, Set<Object>>> iterator() {
                    return new AdHocIterator<Entry<String, Set<Object>>>() {

                        private final Iterator<Entry<String, Set<TObject>>> delegate = data
                                .get(entity).entrySet().iterator();

                        @Override
                        protected Entry<String, Set<Object>> findNext() {
                            Entry<String, Set<TObject>> entry = delegate.next();
                            Set<Object> values = entry.getValue().stream()
                                    .map((item) -> Convert.javaToThrift(item))
                                    .collect(Collectors.toSet());
                            return new SimpleEntry<String, Set<Object>>(
                                    entry.getKey(), values);
                        }

                    };
                }

                @Override
                public int size() {
                    return data.get(entity).size();
                }

            };
        }

        @Override
        public Set<Object> get(Object key) {
            if(key instanceof String) {
                return new LazyTransformSet(entity, (String) key);
            }
            else {
                return null;
            }
        }

        @Override
        public Set<Object> put(String key, Set<Object> value) {
            Set<TObject> stored = data.get(entity, key);
            value.forEach((item) -> data.insert(entity, key,
                    Convert.javaToThrift(item)));
            return stored.stream().map((item) -> Convert.thriftToJava(item))
                    .collect(Collectors.toSet());
        }

        @Override
        public Set<Object> remove(Object key) {
            if(key instanceof String) {
                Set<TObject> stored = data.get(entity, (String) key);
                stored.forEach((value) -> data.delete(entity, (String) key,
                        value));
                return stored.stream()
                        .map((item) -> Convert.thriftToJava(item))
                        .collect(Collectors.toSet());
            }
            else {
                return null;
            }
        }

    }

    /**
     * A wrapper set that transforms written values from Object to TObject and
     * read values from TObject to Object on-demand.
     * 
     * @author Jeff Nelson
     */
    private class LazyTransformSet extends AbstractSet<Object> {

        /**
         * The attribute with which this set is associated.
         */
        private final String attribute;

        /**
         * The entity that owns this set.
         */
        private final long entity;

        /**
         * Construct a new instance.
         * 
         * @param entity
         * @param attribute
         */
        private LazyTransformSet(long entity, String attribute) {
            this.entity = entity;
            this.attribute = attribute;
        }

        @Override
        public boolean add(Object object) {
            return data.insert(entity, attribute, Convert.javaToThrift(object));
        }

        @Override
        public Iterator<Object> iterator() {
            return new AdHocIterator<Object>() {

                Iterator<TObject> delegate = data.get(entity, attribute)
                        .iterator();

                @Override
                protected Object findNext() {
                    return Convert.thriftToJava(delegate.next());
                }

            };
        }

        public boolean remove(Object object) {
            return data.delete(entity, attribute, Convert.javaToThrift(object));
        }

        @Override
        public int size() {
            return data.get(entity, attribute).size();
        }

    }
}
