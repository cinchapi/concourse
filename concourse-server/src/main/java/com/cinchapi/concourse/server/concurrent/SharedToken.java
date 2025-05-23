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
package com.cinchapi.concourse.server.concurrent;

import java.nio.ByteBuffer;

/**
 * A {@link Token} that is intended to be represented by a shared lock (e.g.,
 * {@link SharedReadWriteLock}).
 *
 * @author Jeff Nelson
 */
class SharedToken extends Token {

    /**
     * Construct a new instance.
     * 
     * @param bytes
     */
    protected SharedToken(ByteBuffer bytes) {
        super(bytes);
    }

}
