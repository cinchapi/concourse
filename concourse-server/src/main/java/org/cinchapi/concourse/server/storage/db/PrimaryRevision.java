/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2014 Jeff Nelson, Cinchapi Software Collective
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.cinchapi.concourse.server.storage.db;

import java.nio.ByteBuffer;

import javax.annotation.concurrent.Immutable;

import org.cinchapi.concourse.server.io.Byteable;
import org.cinchapi.concourse.server.model.PrimaryKey;
import org.cinchapi.concourse.server.model.Text;
import org.cinchapi.concourse.server.model.Value;
import org.cinchapi.concourse.server.storage.Action;

/**
 * A {@link Revision} that is used in a {@link PrimaryBlock} and maps a
 * record to a key to a value.
 * 
 * @author jnelson
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