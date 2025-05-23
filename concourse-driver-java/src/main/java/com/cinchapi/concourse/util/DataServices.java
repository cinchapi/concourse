/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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
            .registerTypeAdapterFactory(
                    TypeAdapters.primitiveTypesFactory(true))
            .registerTypeAdapterFactory(TypeAdapters.tObjectFactory(true))
            .registerTypeAdapterFactory(TypeAdapters.collectionFactory(true))
            .registerTypeAdapterFactory(TypeAdapters.functionFactory(true))
            .disableHtmlEscaping().create();

    /**
     * A JsonParser.
     */
    private static final JsonParser JSON_PARSER = new JsonParser();

}
