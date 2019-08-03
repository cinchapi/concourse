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
package com.cinchapi.concourse.server.http;

import java.util.List;

import javax.annotation.Nullable;

import com.google.common.collect.Iterables;
import com.google.common.primitives.Longs;

/**
 * Represents the arguments passed via the route of an HTTP request.
 * <p>
 * The Concourse REST API allows the caller to interchange the order that the
 * key(s) and record(s) appear in a URI. This class helps to parse those args
 * into the proper variables.
 * </p>
 * 
 * @author dubex
 **/
public class RouteArgs {

    /**
     * Parse the objects into the appropriate arguments.
     * 
     * @param objects
     * @return the HttpArgs
     */
    @SuppressWarnings("unchecked")
    public static RouteArgs parse(Object... objects) {
        RouteArgs args = new RouteArgs();
        for (Object obj : objects) {
            if(obj == null) {
                continue;
            }
            else if(obj instanceof List) {
                List<?> list = (List<?>) obj;
                String temp = (String) Iterables.getFirst(list, null);
                if(list.size() > 1) {
                    if(Longs.tryParse(temp) == null) {
                        args.keys = (List<String>) obj;
                    }
                    else {
                        args.records = (List<Long>) obj;
                    }
                }
                else if(list.size() == 1) {
                    if(Longs.tryParse(temp) == null) {
                        args.key = (String) obj;
                    }
                    else {
                        args.record = (Long) obj;
                    }
                }
            }
            else if(obj instanceof String) {
                String str = (String) obj;
                Long record = Longs.tryParse(str);
                if(record != null) {
                    args.record = record;
                }
                else {
                    args.key = str;
                }
            }
        }
        return args;
    }

    /**
     * The single key.
     */
    @Nullable
    private String key = null;

    /**
     * Multiple keys.
     */
    @Nullable
    private List<String> keys = null;

    /**
     * The single record.
     */
    @Nullable
    private Long record = null;

    /**
     * Multiple records.
     */
    @Nullable
    private List<Long> records = null;

    /**
     * Return the key arg, if it exists.
     * 
     * @return the key
     */
    @Nullable
    public String key() {
        return key;
    }

    /**
     * Return the keys arg, if it exists
     * 
     * @return the keys
     */
    public List<String> keys() {
        return keys;
    }

    /**
     * Return the record arg, if it exists.
     * 
     * @return the record
     */
    @Nullable
    public Long record() {
        return record;
    }

    /**
     * Return the records arg, if it exists.
     * 
     * @return the records
     */
    public List<Long> records() {
        return records;
    }

}
