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
package org.cinchapi.concourse.server.concurrent;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.cinchapi.concourse.annotate.PackagePrivate;

/**
 * A custom {@link ReentrantReadWriteLock} that is defined by a {@link Token}.
 * 
 * @author jnelson
 */
@SuppressWarnings("serial")
final class TokenReadWriteLock extends ReferenceCountingLock {

    /**
     * The token that represents the notion this lock controls
     */
    @PackagePrivate
    final Token token;

    /**
     * Construct a new instance.
     * 
     * @param token
     */
    public TokenReadWriteLock(Token token) {
        super(token.cardinality > 1 ? new ReadWriteSharedLock() : new ReentrantReadWriteLock());
        this.token = token;
    }

}