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
package com.cinchapi.concourse.server.io;

import java.nio.ByteBuffer;

/**
 * Utilities for {@link ByteSink ByteSinks}.
 *
 * @author Jeff Nelson
 */
public final class ByteSinks {

    /**
     * Transfer the bytes from {@code source} to {@code destination} and resets
     * {@code source} so that its position remains unchanged. The position of
     * the {@code destination} is incremented by the number of bytes that are
     * transferred.
     * 
     * @param source
     * @param destination
     */
    public static void copyAndRewindSource(ByteBuffer source,
            ByteSink destination) {
        int position = source.position();
        destination.put(source);
        source.position(position);
    }

    private ByteSinks() {/* no-init */}

}
