/*
 * Copyright (c) 2013-2015 Cinchapi, Inc.
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
package org.cinchapi.concourse.util;

import java.io.IOException;

import org.cinchapi.concourse.Link;
import org.cinchapi.concourse.Tag;
import org.cinchapi.concourse.thrift.TObject;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

/**
 * A generic collection of services for dealing with the data.
 * 
 * @author Jeff Nelson
 */
public class DataServices {

    /**
     * Return a serializer for JSON.
     * 
     * @return Gson
     */
    public static Gson gson() {
        return GSON;
    }

    /**
     * A type adapter for serializing TObjects
     * 
     * @author Jeff Nelson
     */
    private static class TObjectTypeAdapter extends TypeAdapter<TObject> {

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
     * A singleton instance of the {@link JavaTypeAdapter}.
     */
    private static JavaTypeAdapter JAVA_TYPE_ADAPTER = new JavaTypeAdapter();

    /**
     * Type adapter for Java objects for JSON serialization.
     * 
     * @author hyin
     */
    private static class JavaTypeAdapter extends TypeAdapter<Object> {
        public Object read(JsonReader reader) throws IOException {
            return null;
        }

        public void write(JsonWriter writer, Object value) throws IOException {
            if(value instanceof Double) {
                value = (Double) value;
                writer.value(value.toString() + "D");
            }
            else if(value instanceof Link) {
                writer.value("@" + value.toString() + "@");
            }
            else if(value instanceof Tag) {
                writer.value("'" + value.toString() + "'");
            }
            else if(value instanceof Number){
                writer.value((Number) value);
            }
            else if(value instanceof Boolean){
                writer.value((Boolean) value);
            }
            else{
                writer.value(value.toString());
            }
        }
    }

    /**
     * THE Gson.
     */
    private static final Gson GSON = new GsonBuilder()
            .registerTypeAdapter(Object.class, JAVA_TYPE_ADAPTER.nullSafe())
            .registerTypeAdapter(TObject.class,
                    new TObjectTypeAdapter().nullSafe()).disableHtmlEscaping()
            .create();

}
