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
package com.cinchapi.concourse.server.concurrent;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.cinchapi.concourse.annotate.PackagePrivate;

/**
 * A custom {@link ReentrantReadWriteLock} that is defined by a {@link Token}.
 * 
 * @author Jeff Nelson
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
        super(token.cardinality == 1 ? new ReadWriteSharedLock() : new ReentrantReadWriteLock());
        this.token = token;
    }

}
