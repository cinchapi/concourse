/*
 * Copyright (c) 2013-2024 Cinchapi Inc.
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.common.io.ByteBuffers;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteSource;
import com.google.common.io.Files;

/**
 * Utility class for generating checksums.
 *
 * @author Jeff Nelson
 */
public final class Checksums {

    /**
     * Generate a checksum for the file at {@code path}.
     * 
     * @param path
     * @return the checksum
     */
    public static String generate(Path path) {
        return generate(Files.asByteSource(path.toFile()));
    }

    /**
     * Generate a checksum for the content in the {@code source}.
     * 
     * @param source
     * @return the checksum
     */
    public static String generate(ByteBuffer source) {
        return generate(ByteSource.wrap(ByteBuffers.getByteArray(source)));
    }

    /**
     * Generate a checksum for the content in the {@code source}.
     * 
     * @param source
     * @return the checksum
     */
    public static String generate(ByteSource source) {
        try {
            return source.hash(Hashing.murmur3_128()).toString();
        }
        catch (IOException e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
    }

    private Checksums() {/* no-init */}
}
