/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse.server.storage;

/**
 * A class that can retrieve the version for a record or a key+record.
 * 
 * @author Jeff Nelson
 */
public interface VersionGetter {

    /**
     * Return the current version of {@code record}.
     * 
     * @param record
     * @return the version
     */
    public long getVersion(long record);

    /**
     * Return the current version of {@code key}.
     * 
     * @param key
     * @return the version
     */
    public long getVersion(String key);

    /**
     * Return the current version of {@code key} in {@code record}
     * 
     * @param key
     * @param record
     * @return the version
     */
    public long getVersion(String key, long record);

}
