/*
 * Copyright (c) 2013-2021 Cinchapi Inc.
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

import com.cinchapi.ccl.type.Function;
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
     * Return a {@link TypeAdapterFactory} that contains the preferred JSON
     * de/serialization rules for {@link Collection Collections}.
     * 
     * @return the type adapter factory
     */
    public static TypeAdapterFactory collectionFactory() {
        return collectionFactory(false);
    }

    /**
     * Return a {@link TypeAdapterFactory} that contains the preferred JSON
     * de/serialization rules for {@link Collection Collections}.
     * 
     * @param nullSafe
     * @return the type adapter factory
     */
    public static TypeAdapterFactory collectionFactory(boolean nullSafe) {
        return new TypeAdapterFactory() {

            @SuppressWarnings("unchecked")
            @Override
            public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
                TypeAdapter<T> adapter = null;
                Class<? super T> clazz = type.getRawType();
                TypeAdapterFactory skipPast = this;
                if(Collection.class.isAssignableFrom(clazz)) {
                    adapter = (TypeAdapter<T>) new TypeAdapter<Collection<?>>() {

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
                if(adapter != null && nullSafe) {
                    adapter = adapter.nullSafe();
                }
                return adapter;
            }

        };
    }

    /**
     * Return a {@link TypeAdapterFactory} that contains the preferred JSON
     * de/serialization rules for {@link Function Functions}.
     * 
     * @return the type adapter factory
     */
    public static TypeAdapterFactory functionFactory() {
        return functionFactory(false);
    }

    /**
     * Return a {@link TypeAdapterFactory} that contains the preferred JSON
     * de/serialization rules for {@link Function Functions}.
     * 
     * @param nullSafe
     * @return the type adapter factory
     */
    public static TypeAdapterFactory functionFactory(boolean nullSafe) {
        return new TypeAdapterFactory() {

            @SuppressWarnings("unchecked")
            @Override
            public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
                TypeAdapter<T> adapter = null;
                Class<? super T> clazz = type.getRawType();
                if(Function.class.isAssignableFrom(clazz)) {
                    adapter = (TypeAdapter<T>) new TypeAdapter<Function>() {

                        @Override
                        public Function read(JsonReader in) throws IOException {
                            return null;
                        }

                        @Override
                        public void write(JsonWriter out, Function value)
                                throws IOException {
                            out.value(value.toString());
                        }

                    };
                }
                if(adapter != null && nullSafe) {
                    adapter = adapter.nullSafe();
                }
                return adapter;
            }

        };
    }

    /**
     * Return a {@link TypeAdapterFactory} that contains the preferred JSON
     * de/serialization rules for primitive Concourse types.
     * 
     * @return the type adapter factory
     */
    public static TypeAdapterFactory primitiveTypesFactory() {
        return primitiveTypesFactory(false);
    }

    /**
     * Return a {@link TypeAdapterFactory} that contains the preferred JSON
     * de/serialization rules for primitive Concourse types.
     * 
     * @param nullSafe
     * @return the type adapter factory
     */
    public static TypeAdapterFactory primitiveTypesFactory(boolean nullSafe) {
        return new TypeAdapterFactory() {

            @SuppressWarnings("unchecked")
            @Override
            public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
                TypeAdapter<T> adapter = null;
                Class<? super T> clazz = type.getRawType();
                if(Double.class.isAssignableFrom(clazz)
                        || double.class.isAssignableFrom(clazz)) {
                    adapter = (TypeAdapter<T>) new TypeAdapter<Double>() {

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
                    adapter = (TypeAdapter<T>) new TypeAdapter<Link>() {

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
                    adapter = (TypeAdapter<T>) new TypeAdapter<Tag>() {

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
                    adapter = (TypeAdapter<T>) new TypeAdapter<Timestamp>() {

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
                if(adapter != null && nullSafe) {
                    adapter = adapter.nullSafe();
                }
                return adapter;
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
        return tObjectFactory(false);
    }

    /**
     * Return a {@link TypeAdapterFactory} that contains the preferred JSON
     * de/serialization rules for {@link TObject TObjects}.
     * 
     * @param nullSafe
     * @return the type adapter factory
     */
    public static TypeAdapterFactory tObjectFactory(boolean nullSafe) {
        return new TypeAdapterFactory() {

            @SuppressWarnings("unchecked")
            @Override
            public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
                TypeAdapter<T> adapter = null;
                Class<? super T> clazz = type.getRawType();
                TypeAdapterFactory skipPast = this;
                if(TObject.class.isAssignableFrom(clazz)) {
                    adapter = (TypeAdapter<T>) new TypeAdapter<TObject>() {

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
                if(adapter != null && nullSafe) {
                    adapter = adapter.nullSafe();
                }
                return adapter;
            }

        };
    }

}
