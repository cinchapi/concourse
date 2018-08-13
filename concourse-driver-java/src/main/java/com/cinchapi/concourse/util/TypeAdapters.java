/*
 * Copyright (c) 2013-2018 Cinchapi Inc.
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
package com.cinchapi.concourse.util;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import com.cinchapi.concourse.Link;
import com.cinchapi.concourse.Tag;
import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.thrift.TObject;
import com.google.common.collect.Iterables;
import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

/**
 * A collection of {@link TypeAdapter} factories to use when converting java
 * objects to their appropriate JSON representation.
 * 
 * @author Jeff Nelson
 */
public class TypeAdapters {

    /**
     * A singleton instance of the type adapter for collections to use within
     * this class.
     */
    @Deprecated
    private static TypeAdapter<Collection<?>> COLLECTION_TYPE_ADAPTER = new TypeAdapter<Collection<?>>() {

        @Override
        public Collection<?> read(JsonReader in) throws IOException {
            return null;
        }

        @Override
        public void write(JsonWriter out, Collection<?> value)
                throws IOException {
            // TODO: There is an open question about how to handle empty
            // collections. Right now, an empty JSON array is output, but
            // maybe we want to output null instead?
            if(value.size() == 1) {
                sendJsonValue(out, Iterables.get(value, 0));
            }
            else {
                out.beginArray();
                for (Object element : value) {
                    sendJsonValue(out, element);
                }
                out.endArray();
            }

        }
    };

    /**
     * A singleton instance of the type adapter for generic objects to use
     * within this class.
     */
    @Deprecated
    private static TypeAdapter<Object> JAVA_TYPE_ADAPTER = new TypeAdapter<Object>() {

        /**
         * A generic type adapter from a standard GSON instance that is used for
         * maintaining default deserialization semantics for non-primitive
         * objects.
         */
        private final TypeAdapter<Object> generic = new Gson()
                .getAdapter(Object.class);

        @Override
        public Object read(JsonReader reader) throws IOException {
            return null;
        }

        @Override
        public void write(JsonWriter writer, Object value) throws IOException {
            if(value instanceof Double) {
                value = (Double) value;
                writer.value(value.toString() + "D");
            }
            else if(value instanceof Link) {
                writer.value(value.toString());
            }
            else if(value instanceof Tag) {
                writer.value("'" + value.toString() + "'");
            }
            else if(value instanceof Number) {
                writer.value((Number) value);
            }
            else if(value instanceof Boolean) {
                writer.value((Boolean) value);
            }
            else if(value instanceof String) {
                writer.value((String) value);
            }
            else if(value instanceof Timestamp) {
                writer.value("|" + ((Timestamp) value).getMicros() + "|");
            }
            else {
                generic.write(writer, value);
            }
        }
    };

    /**
     * A singleton instance to return from {@link #forMap()}.
     */
    @Deprecated
    private static TypeAdapter<Map<?, ?>> MAP_TYPE_ADAPTER = new TypeAdapter<Map<?, ?>>() {

        @Override
        public Map<String, ?> read(JsonReader in) throws IOException {
            return null;
        }

        @Override
        public void write(JsonWriter out, Map<?, ?> value) throws IOException {
            out.beginObject();
            for (Entry<?, ?> entry : value.entrySet()) {
                out.name(entry.getKey().toString());
                sendJsonValue(out, entry.getValue());
            }
            out.endObject();

        }

    };

    /**
     * A singleton instance of the type adapter for tobjects to use within this
     * class.
     */
    @Deprecated
    private static TypeAdapter<TObject> TOBJECT_TYPE_ADAPTER = new TypeAdapter<TObject>() {

        @Override
        public TObject read(JsonReader in) throws IOException {
            return null;
        }

        @Override
        public void write(JsonWriter out, TObject value) throws IOException {
            JAVA_TYPE_ADAPTER.write(out, Convert.thriftToJava(value));
        }
    };

    /**
     * Return a {@link TypeAdapterFactory} that contains the preferred JSON
     * de/serialization rules for {@link Collection Collections}.
     * 
     * @return the type adapter factory
     */
    public static TypeAdapterFactory collectionFactory() {
        return new TypeAdapterFactory() {

            @SuppressWarnings("unchecked")
            @Override
            public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
                Class<? super T> clazz = type.getRawType();
                TypeAdapterFactory skipPast = this;
                if(Collection.class.isAssignableFrom(clazz)) {
                    return (TypeAdapter<T>) new TypeAdapter<Collection<?>>() {

                        @Override
                        public Collection<?> read(JsonReader in)
                                throws IOException {
                            return null;
                        }

                        @SuppressWarnings("rawtypes")
                        @Override
                        public void write(JsonWriter out, Collection<?> value)
                                throws IOException {
                            // Delegate to the normal Collection adapter unless
                            // the collection only contains one item; in which
                            // case, it should delegate to the normal adapter
                            // for that item, so it can be written as a flat
                            // value.
                            Object item;
                            TypeAdapter delegate;
                            if(value.size() == 1) {
                                item = Iterables.getOnlyElement(value);
                                delegate = (TypeAdapter<?>) gson
                                        .getDelegateAdapter(skipPast,
                                                TypeToken.get(item.getClass()));

                            }
                            else {
                                delegate = gson.getDelegateAdapter(skipPast,
                                        type);
                                item = value;
                            }
                            delegate.write(out, item);
                        }

                    };
                }
                else {
                    return null;
                }
            }

        };
    }

    /**
     * Return the {@link TypeAdapter} that checks the size of a collection and
     * only converts them to a JSON array if necessary (i.e. a single item
     * collection will be converted to a single object in JSON).
     * 
     * @return the {@link TypeAdapter} to use for conversions
     * @deprecated The use of the type adapter is flawed because it doesn't
     *             leverage {@link Gson}'s ability to dynamically override
     *             adapters from different sources. The preferred approach is to
     *             pass {@link #primitiveTypesFactory()} to
     *             {@link com.google.gson.GsonBuilder#registerTypeAdapterFactory(TypeAdapterFactory)}
     *             instead.
     */
    @Deprecated
    public static TypeAdapter<Collection<?>> forCollection() {
        return COLLECTION_TYPE_ADAPTER;
    }

    /**
     * Return the {@link TypeAdapter} that converts generic objects to the
     * correct JSON representation so they can be converted back to java if
     * necessary.
     * 
     * @return the {@link TypeAdapter} to use for conversions
     * @deprecated The use of the type adapter is flawed because it doesn't
     *             leverage {@link Gson}'s ability to dynamically override
     *             adapters from different sources. The preferred approach is to
     *             pass {@link #primitiveTypesFactory()} to
     *             {@link com.google.gson.GsonBuilder#registerTypeAdapterFactory(TypeAdapterFactory)}
     *             instead.
     */
    @Deprecated
    public static TypeAdapter<Object> forGenericObject() {
        return JAVA_TYPE_ADAPTER;
    }

    /**
     * Return the {@link TypeAdapter} that converts built-in {@link Map maps} to
     * the correct JSON representation by deferring to the
     * {@link #forGenericObject() java type adapter} when serializing values.
     * This enables Concourse Server to correct convert the resultant JSON
     * string back to a Java structure when necessary.
     * 
     * @return the {@link TypeAdapter} to use for conversions
     * @deprecated The use of the type adapter is flawed because it doesn't
     *             leverage {@link Gson}'s ability to dynamically override
     *             adapters from different sources. The preferred approach is to
     *             pass {@link #primitiveTypesFactory()} to
     *             {@link com.google.gson.GsonBuilder#registerTypeAdapterFactory(TypeAdapterFactory)}
     *             instead.
     */
    @Deprecated
    public static TypeAdapter<Map<?, ?>> forMap() {
        return MAP_TYPE_ADAPTER;
    }

    /**
     * Return the {@link TypeAdapter} that converts {@link TObject TObjects} to
     * the correct JSON representation so they can be converted back to java if
     * necessary.
     * 
     * @return the {@link TypeAdapter} to use for conversions
     * @deprecated The use of the type adapter is flawed because it doesn't
     *             leverage {@link Gson}'s ability to dynamically override
     *             adapters from different sources. The preferred approach is to
     *             pass {@link #primitiveTypesFactory()} to
     *             {@link com.google.gson.GsonBuilder#registerTypeAdapterFactory(TypeAdapterFactory)}
     *             instead.
     */
    @Deprecated
    public static TypeAdapter<TObject> forTObject() {
        return TOBJECT_TYPE_ADAPTER;
    }

    /**
     * Return a {@link TypeAdapterFactory} that contains the preferred JSON
     * de/serialization rules for primitive Concourse types.
     * 
     * @return the type adapter factory
     */
    public static TypeAdapterFactory primitiveTypesFactory() {
        return new TypeAdapterFactory() {

            @SuppressWarnings("unchecked")
            @Override
            public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
                Class<? super T> clazz = type.getRawType();
                if(Double.class.isAssignableFrom(clazz)
                        || double.class.isAssignableFrom(clazz)) {
                    return (TypeAdapter<T>) new TypeAdapter<Double>() {

                        @Override
                        public Double read(JsonReader in) throws IOException {
                            return null;
                        }

                        @Override
                        public void write(JsonWriter out, Double value)
                                throws IOException {
                            // This adapter appends the value with a "D" to
                            // force Concourse to reread them as a {@link
                            // Double} type instead of a {@link Float}.
                            out.value(value.toString() + "D");
                        }

                    };
                }
                else if(Link.class.isAssignableFrom(clazz)) {
                    return (TypeAdapter<T>) new TypeAdapter<Link>() {

                        @Override
                        public Link read(JsonReader in) throws IOException {
                            return null;
                        }

                        @Override
                        public void write(JsonWriter out, Link value)
                                throws IOException {
                            out.value(value.toString());
                        }

                    };
                }
                else if(Tag.class.isAssignableFrom(clazz)) {
                    return (TypeAdapter<T>) new TypeAdapter<Tag>() {

                        @Override
                        public Tag read(JsonReader in) throws IOException {
                            return null;
                        }

                        @Override
                        public void write(JsonWriter out, Tag value)
                                throws IOException {
                            out.value("'" + value.toString() + "'");
                        }

                    };
                }
                else if(Timestamp.class.isAssignableFrom(clazz)) {
                    return (TypeAdapter<T>) new TypeAdapter<Timestamp>() {

                        @Override
                        public Timestamp read(JsonReader in)
                                throws IOException {
                            return null;
                        }

                        @Override
                        public void write(JsonWriter out, Timestamp value)
                                throws IOException {
                            out.value("|" + ((Timestamp) value).getMicros()
                                    + "|");
                        }

                    };
                }
                else {
                    return null;
                }
            }

        };
    }

    /**
     * Return a {@link TypeAdapterFactory} that contains the preferred JSON
     * de/serialization rules for {@link TObject TObjects}.
     * 
     * @return the type adapter factory
     */
    public static TypeAdapterFactory tObjectFactory() {
        return new TypeAdapterFactory() {

            @SuppressWarnings("unchecked")
            @Override
            public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
                Class<? super T> clazz = type.getRawType();
                TypeAdapterFactory skipPast = this;
                if(TObject.class.isAssignableFrom(clazz)) {
                    return (TypeAdapter<T>) new TypeAdapter<TObject>() {

                        @Override
                        public TObject read(JsonReader in) throws IOException {
                            return null;
                        }

                        @SuppressWarnings("rawtypes")
                        @Override
                        public void write(JsonWriter out, TObject value)
                                throws IOException {
                            // Convert to Java and use the converted type's
                            // delegate for JSON serialization
                            Object java = Convert.thriftToJava(value);
                            TypeAdapter delegate = gson.getDelegateAdapter(
                                    skipPast, TypeToken.get(java.getClass()));
                            delegate.write(out, java);
                        }

                    };
                }
                else {
                    return null;
                }
            }

        };
    }

    /**
     * Check the type of {@code value} and send it the appropriate type
     * adapter with {@code out}.
     * 
     * @param out the writer
     * @param value the value to write
     * @throws IOException
     */
    @Deprecated
    private static void sendJsonValue(JsonWriter out, Object value)
            throws IOException {
        if(value instanceof TObject) {
            TOBJECT_TYPE_ADAPTER.write(out, (TObject) value);
        }
        else if(value instanceof Collection) {
            COLLECTION_TYPE_ADAPTER.write(out, (Collection<?>) value);
        }
        else if(value instanceof Map) {
            MAP_TYPE_ADAPTER.write(out, (Map<?, ?>) value);
        }
        else {
            JAVA_TYPE_ADAPTER.write(out, value);
        }
    }

}
