/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2015 Jeff Nelson, Cinchapi Software Collective
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
import org.cinchapi.concourse.server.model.Position;
import org.cinchapi.concourse.server.model.Text;
import org.cinchapi.concourse.server.storage.Action;

/**
 * A {@link Revision} that is used in a {@link SearchBlock} and maps a key
 * to a term to a position.
 * 
 * @author jnelson
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