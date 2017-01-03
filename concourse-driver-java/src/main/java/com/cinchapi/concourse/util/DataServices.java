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

import java.util.Collection;
import java.util.Map;

import com.cinchapi.concourse.thrift.TObject;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;

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
     * Return a parser for JSON.
     * 
     * @return Json parser
     */
    public static JsonParser jsonParser() {
        return JSON_PARSER;
    }

    /**
     * THE Gson.
     */
    private static final Gson GSON = new GsonBuilder()
            .registerTypeAdapter(Object.class,
                    TypeAdapters.forGenericObject().nullSafe())
            .registerTypeAdapter(TObject.class,
                    TypeAdapters.forTObject().nullSafe())
            .registerTypeHierarchyAdapter(Collection.class,
                    TypeAdapters.forCollection().nullSafe())
            .registerTypeHierarchyAdapter(Map.class,
                    TypeAdapters.forMap().nullSafe()).disableHtmlEscaping()
            .create();

    /**
     * A JsonParser.
     */
    private static final JsonParser JSON_PARSER = new JsonParser();

}
