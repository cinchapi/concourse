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
package com.cinchapi.concourse.server.storage.db;

import java.nio.ByteBuffer;

import javax.annotation.concurrent.Immutable;

import com.cinchapi.concourse.server.io.Byteable;
import com.cinchapi.concourse.server.model.PrimaryKey;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.server.storage.Action;

/**
 * A {@link Revision} that is used in a {@link PrimaryBlock} and maps a
 * record to a key to a value.
 * 
 * @author Jeff Nelson
 */
@Immutable
public final class PrimaryRevision extends Revision<PrimaryKey, Text, Value> {

    /**
     * Construct an instance that represents an existing PrimaryRevision
     * from a ByteBuffer. This constructor is public so as to comply with
     * the {@link Byteable} interface. Calling this constructor directly is
     * not recommend.
     * 
     * @param bytes
     */
    private PrimaryRevision(ByteBuffer bytes) {
        super(bytes);
    }

    /**
     * Construct a new instance.
     * 
     * @param locator
     * @param key
     * @param value
     * @param version
     * @param type
     */
    PrimaryRevision(PrimaryKey locator, Text key, Value value, long version,
            Action type) {
        super(locator, key, value, version, type);
    }

    @Override
    protected Class<Text> xKeyClass() {
        return Text.class;
    }

    @Override
    protected int xKeySize() {
        return VARIABLE_SIZE;
    }

    @Override
    protected Class<PrimaryKey> xLocatorClass() {
        return PrimaryKey.class;
    }

    @Override
    protected int xLocatorSize() {
        return PrimaryKey.SIZE;
    }

    @Override
    protected Class<Value> xValueClass() {
        return Value.class;
    }

}
