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
package com.cinchapi.concourse.util;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import com.cinchapi.concourse.Link;
import com.cinchapi.concourse.Tag;
import com.cinchapi.concourse.thrift.TObject;
import com.google.common.collect.Iterables;
import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
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
     * Return the {@link TypeAdapter} that checks the size of a collection and
     * only converts them to a JSON array if necessary (i.e. a single item
     * collection will be converted to a single object in JSON).
     * 
     * @return the {@link TypeAdapter} to use for conversions
     */
    public static TypeAdapter<Collection<?>> forCollection() {
        return COLLECTION_TYPE_ADAPTER;
    }

    /**
     * Return the {@link TypeAdapter} that converts generic objects to the
     * correct JSON representation so they can be converted back to java if
     * necessary.
     * 
     * @return the {@link TypeAdapter} to use for conversions
     */
    public static TypeAdapter<Object> forGenericObject() {
        return JAVA_TYPE_ADAPTER;
    }

    /**
     * Return the {@link TypeAdapter} that converts {@link TObject TObjects} to
     * the correct JSON representation so they can be converted back to java if
     * necessary.
     * 
     * @return the {@link TypeAdapter} to use for conversions
     */
    public static TypeAdapter<TObject> forTObject() {
        return TOBJECT_TYPE_ADAPTER;
    }

    /**
     * Return the {@link TypeAdapter} that converts built-in {@link Map maps} to
     * the correct JSON representation by deferring to the
     * {@link #forGenericObject() java type adapter} when serializing values.
     * This enables Concourse Server to correct convert the resultant JSON
     * string back to a Java structure when necessary.
     * 
     * @return the {@link TypeAdapter} to use for conversions
     */
    public static TypeAdapter<Map<?, ?>> forMap() {
        return MAP_TYPE_ADAPTER;
    }

    /**
     * A singleton instance of the type adapter for generic objects to use
     * within this class.
     */
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
            else {
                generic.write(writer, value);
            }
        }
    };

    /**
     * A singleton instance of the type adapter for tobjects to use within this
     * class.
     */
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
     * A singleton instance to return from {@link #forMap()}.
     */
    private static TypeAdapter<Map<?, ?>> MAP_TYPE_ADAPTER = new TypeAdapter<Map<?, ?>>() {

        @Override
        public void write(JsonWriter out, Map<?, ?> value) throws IOException {
            out.beginObject();
            for (Entry<?, ?> entry : value.entrySet()) {
                out.name(entry.getKey().toString());
                sendJsonValue(out, entry.getValue());
            }
            out.endObject();

        }

        @Override
        public Map<String, ?> read(JsonReader in) throws IOException {
            return null;
        }

    };

    /**
     * A singleton instance of the type adapter for collections to use within
     * this class.
     */
    private static TypeAdapter<Collection<?>> COLLECTION_TYPE_ADAPTER = new TypeAdapter<Collection<?>>() {

        @Override
        public Collection<?> read(JsonReader in) throws IOException {
            return null;
        }

        @Override
        public void write(JsonWriter out, Collection<?> value)
                throws IOException {
            // TODO: There is an open question about how to handle empty
            // collections. Right now, an empty JSON array is outputed, but
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
     * Check the type of {@code value} and send it the appropriate type
     * adapter with {@code out}.
     * 
     * @param out the writer
     * @param value the value to write
     * @throws IOException
     */
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
