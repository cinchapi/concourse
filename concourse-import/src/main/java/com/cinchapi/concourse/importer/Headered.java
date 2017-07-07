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
package com.cinchapi.concourse.importer;

/**
 * A marker interface for an {@link Importer} that makes use of a header to
 * define the fields into which data is imported.
 * 
 * @author Jeff Nelson
 */
public interface Headered {

    /**
     * After parsing the {@code line}, assign each of the tokens to be the
     * header for the {@link Importer} if and only if a header has not already
     * been assigned or parsed.
     * 
     * @param line the line that should be parsed to get the header
     * @throws IllegalStateException if a header already exists
     */
    public void parseHeader(String line);
}
