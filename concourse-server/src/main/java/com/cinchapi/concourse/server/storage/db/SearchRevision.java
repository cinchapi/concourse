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
import com.cinchapi.concourse.server.model.Position;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.storage.Action;

/**
 * A {@link Revision} that is used in a {@link SearchBlock} and maps a key
 * to a term to a position.
 * 
 * @author Jeff Nelson
 */
@Immutable
public final class SearchRevision extends Revision<Text, Text, Position> {

    /**
     * Construct an instance that represents an existing SearchRevision from
     * a ByteBuffer. This constructor is public so as to comply with the
     * {@link Byteable} interface. Calling this constructor directly is not
     * recommend.
     * 
     * @param bytes
     */
    private SearchRevision(ByteBuffer bytes) {
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
    SearchRevision(Text locator, Text key, Position value, long version,
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
    protected Class<Text> xLocatorClass() {
        return Text.class;
    }

    @Override
    protected int xLocatorSize() {
        return VARIABLE_SIZE;
    }

    @Override
    protected Class<Position> xValueClass() {
        return Position.class;
    }

}
