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
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.cinchapi.common.base.AdHocIterator;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.thrift.Type;
import com.cinchapi.concourse.util.Convert;
import com.google.common.collect.Maps;

/**
 * A {@link ResultDataset} that wraps a {@link TObjectDataset} and lazily
 * transforms values while continuing to write through to the underlying
 * dataset.
 * 
 * @author Jeff Nelson
 */
public class ObjectResultDataset extends ResultDataset<Object> {

    /**
     * The internal dataset that contains the data.
     */
    protected Dataset<Long, String, TObject> thrift;

    /**
     * Construct a new instance.
     * 
     * @param thrift
     */
    public ObjectResultDataset(Dataset<Long, String, TObject> thrift) {
        this.thrift = thrift;
    }

    @Override
    public boolean delete(Long entity, String attribute, Object value) {
        return thrift.delete(entity, attribute, Convert.javaToThrift(value));
    }

    @Override
    public Set<Entry<Long, Map<String, Set<Object>>>> entrySet() {
        return new AbstractSet<Entry<Long, Map<String, Set<Object>>>>() {

            @Override
            public Iterator<Entry<Long, Map<String, Set<Object>>>> iterator() {
                Iterator<Entry<Long, Map<String, Set<TObject>>>> it = thrift
                        .entrySet().iterator();
                return new AdHocIterator<Entry<Long, Map<String, Set<Object>>>>() {

                    @Override
                    protected Entry<Long, Map<String, Set<Object>>> findNext() {
                        if(it.hasNext()) {
                            Entry<Long, Map<String, Set<TObject>>> next = it
                                    .next();
                            long key = next.getKey();
                            Map<String, Set<Object>> value = get(key);
                            return new SimpleEntry<>(key, value);
                        }
                        else {
                            return null;
                        }
                    }

                };
            }

            @Override
            public int size() {
                return thrift.entrySet().size();
            }

        };
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof ObjectResultDataset) {
            return thrift.equals(((ObjectResultDataset) obj).thrift);
        }
        else {
            return false;
        }
    }

    @Override
    public Set<Object> get(Long entity, String attribute) {
        return new AbstractSet<Object>() {

            @Override
            public boolean add(Object e) {
                return insert(entity, attribute, e);
            }

            @Override
            public boolean contains(Object o) {
                return thrift.get(entity, attribute).contains(
                        Convert.javaToThrift(o));
            }

            @Override
            public Iterator<Object> iterator() {
                return new AdHocIterator<Object>() {

                    Iterator<TObject> it = thrift.get(entity, attribute)
                            .iterator();

                    @Override
                    protected Object findNext() {
                        if(it.hasNext()) {
                            return Convert.thriftToJava(it.next());
                        }
                        else {
                            return false;
                        }
                    }

                };
            }

            @Override
            public boolean remove(Object o) {
                return delete(entity, attribute, o);
            }

            @Override
            public int size() {
                return thrift.get(entity, attribute).size();
            }

        };
    }

    @Override
    public Map<String, Set<Object>> get(Object entity) {
        if(entity instanceof Long) {
            return new AbstractMap<String, Set<Object>>() {

                @Override
                public Set<Entry<String, Set<Object>>> entrySet() {
                    return new AbstractSet<Entry<String, Set<Object>>>() {

                        @Override
                        public Iterator<Entry<String, Set<Object>>> iterator() {
                            Iterator<Entry<String, Set<TObject>>> it = thrift
                                    .get(entity).entrySet().iterator();
                            return new AdHocIterator<Entry<String, Set<Object>>>() {

                                @Override
                                protected Entry<String, Set<Object>> findNext() {
                                    if(it.hasNext()) {
                                        Entry<String, Set<TObject>> entry = it
                                                .next();
                                        return new SimpleEntry<>(
                                                entry.getKey(),
                                                entry.getValue()
                                                        .stream()
                                                        .map((value) -> Convert
                                                                .thriftToJava(value))
                                                        .collect(
                                                                Collectors
                                                                        .toSet()));
                                    }
                                    else {
                                        return null;
                                    }
                                }

                            };
                        }

                        @Override
                        public int size() {
                            return thrift.get(entity).size();
                        }

                    };
                }

                @Override
                public Set<Object> get(Object key) {
                    if(key instanceof String) {
                        String attribute = (String) key;
                        return new AbstractSet<Object>() {

                            @Override
                            public boolean add(Object e) {
                                return insert((Long) entity, attribute, e);
                            }

                            @Override
                            public Iterator<Object> iterator() {
                                return new AdHocIterator<Object>() {

                                    Iterator<TObject> it = thrift.get(
                                            (Long) entity, attribute)
                                            .iterator();

                                    @Override
                                    protected Object findNext() {
                                        if(it.hasNext()) {
                                            return Convert.thriftToJava(it
                                                    .next());
                                        }
                                        else {
                                            return null;
                                        }
                                    }

                                };
                            }

                            @Override
                            public boolean remove(Object o) {
                                return delete((Long) entity, attribute, o);
                            }

                            @Override
                            public int size() {
                                return thrift.get((Long) entity, attribute)
                                        .size();
                            }

                        };
                    }
                    else {
                        return null;
                    }
                }

                @Override
                public Set<Object> put(String key, Set<Object> value) {
                    Set<Object> stored = thrift.get((Long) entity, key)
                            .stream().map((v) -> Convert.thriftToJava(v))
                            .collect(Collectors.toSet());
                    value.forEach(v -> insert((Long) entity, key, v));
                    return stored;
                }

                @Override
                public Set<Object> remove(Object key) {
                    if(key instanceof String) {
                        String attribute = (String) key;
                        Set<Object> stored = thrift
                                .get((Long) entity, attribute).stream()
                                .map((v) -> Convert.thriftToJava(v))
                                .collect(Collectors.toSet());
                        return stored;
                    }
                    else {
                        return null;
                    }
                }

            };
        }
        else {
            return null;
        }
    }

    @Override
    public int hashCode() {
        return thrift.hashCode();
    }

    @Override
    public boolean insert(Long entity, String attribute, Object value) {
        return thrift.insert(entity, attribute, Convert.javaToThrift(value));
    }

    @Override
    public Map<String, Map<Object, Set<Long>>> invert() {
        return new AbstractMap<String, Map<Object, Set<Long>>>() {

            @Override
            public Set<Entry<String, Map<Object, Set<Long>>>> entrySet() {
                return new AbstractSet<Entry<String, Map<Object, Set<Long>>>>() {

                    @Override
                    public Iterator<Entry<String, Map<Object, Set<Long>>>> iterator() {
                        final Iterator<String> it = thrift.invert().keySet()
                                .iterator();
                        return new AdHocIterator<Entry<String, Map<Object, Set<Long>>>>() {

                            @Override
                            protected Entry<String, Map<Object, Set<Long>>> findNext() {
                                if(it.hasNext()) {
                                    String attribute = it.next();
                                    return new SimpleEntry<>(attribute,
                                            invert(attribute));
                                }
                                else {
                                    return null;
                                }
                            }

                        };
                    }

                    @Override
                    public int size() {
                        return invert().size();
                    }

                };
            }

            @Override
            public Map<Object, Set<Long>> get(Object attribute) {
                if(attribute instanceof String) {
                    return invert((String) attribute);
                }
                else {
                    return null;
                }
            }

            @Override
            public Map<Object, Set<Long>> put(String attribute,
                    Map<Object, Set<Long>> inverted) {
                Map<Object, Set<Long>> stored = Maps.newLinkedHashMap();
                stored.putAll(get(attribute));
                inverted.forEach((value, entities) -> invert(attribute).put(
                        value, entities));
                return stored;
            }

            @Override
            public Map<Object, Set<Long>> remove(Object key) {
                if(key instanceof String) {
                    String attribute = (String) key;
                    Map<Object, Set<Long>> stored = Maps.newLinkedHashMap();
                    stored.putAll(get(attribute));
                    stored.forEach((value, entities) -> entities.forEach((
                            entity) -> thrift.delete(entity, attribute,
                            Convert.javaToThrift(value))));
                    return stored;
                }
                else {
                    return null;
                }
            }

        };
    }

    @Override
    public Map<Object, Set<Long>> invert(String attribute) {
        return new TrackingMultimap<Object, Long>(Collections.emptyMap()) {

            @Override
            public boolean containsDataType(DataType type) {
                return ((TrackingMultimap<TObject, Long>) thrift
                        .invert(attribute)).containsDataType(type);
            }

            @Override
            public boolean delete(Object value, Long entity) {
                return thrift.delete(entity, attribute,
                        Convert.javaToThrift(value));
            }

            @Override
            public Set<Entry<Object, Set<Long>>> entrySet() {
                return new AbstractSet<Entry<Object, Set<Long>>>() {

                    @Override
                    public Iterator<Entry<Object, Set<Long>>> iterator() {
                        final Iterator<Entry<TObject, Set<Long>>> it = thrift
                                .invert(attribute).entrySet().iterator();
                        return new AdHocIterator<Entry<Object, Set<Long>>>() {

                            @Override
                            protected Entry<Object, Set<Long>> findNext() {
                                if(it.hasNext()) {
                                    Entry<TObject, Set<Long>> entry = it.next();
                                    return new SimpleEntry<>(
                                            Convert.thriftToJava(entry.getKey()),
                                            entry.getValue());
                                }
                                else {
                                    return null;
                                }
                            }

                        };
                    }

                    @Override
                    public int size() {
                        return thrift.invert(attribute).size();
                    }

                };
            }

            @Override
            public boolean equals(Object obj) {
                if(this.getClass() == obj.getClass()) {
                    Object entrySet = Reflection.call(obj, "entrySet");
                    return entrySet().equals(entrySet);
                }
                else {
                    return false;
                }
            }

            @Override
            public Set<Long> get(Object value) {
                return thrift.invert(attribute)
                        .get(Convert.javaToThrift(value));
            }

            @Override
            public int hashCode() {
                return thrift.invert(attribute).hashCode();
            }

            @Override
            public boolean hasValue(Long value) {
                return ((TrackingMultimap<TObject, Long>) thrift
                        .invert(attribute)).hasValue(value);
            }

            @Override
            public boolean insert(Object value, Long entity) {
                return thrift.insert(entity, attribute,
                        Convert.javaToThrift(value));
            }

            @Override
            public Set<Long> merge(Object value, Set<Long> entities) {
                return ((TrackingMultimap<TObject, Long>) thrift
                        .invert(attribute)).merge(Convert.javaToThrift(value),
                        entities);
            }

            @Override
            public double percentKeyDataType(DataType type) {
                return ((TrackingMultimap<TObject, Long>) thrift
                        .invert(attribute)).percentKeyDataType(type);
            }

            @Override
            public double proportion(Object value) {
                return ((TrackingMultimap<TObject, Long>) thrift
                        .invert(attribute)).proportion(Convert
                        .javaToThrift(value));
            }

            @Override
            public Set<Long> put(Object value, Set<Long> entities) {
                return thrift.invert(attribute).put(
                        Convert.javaToThrift(value), entities);
            }

            @Override
            public Set<Long> remove(Object value) {
                return thrift.invert(attribute).remove(
                        Convert.javaToThrift(value));
            }

            @Override
            public String toString() {
                return thrift.invert(attribute).toString();
            }

            @Override
            public double uniqueness() {
                return ((TrackingMultimap<TObject, Long>) thrift
                        .invert(attribute)).uniqueness();
            }

            @Override
            public VariableType variableType() {
                return ((TrackingMultimap<TObject, Long>) thrift
                        .invert(attribute)).variableType();
            }

            @Override
            protected Set<Long> createValueSet() {
                // NOTE: this won't ever be called because all wrties are routed
                // to the underyling #thrift based collection
                return null;
            }

        };
    }

    @Override
    public String toString() {
        return thrift.toString();
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

}
