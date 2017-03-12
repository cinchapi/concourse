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
package com.cinchapi.concourse.util;

import java.io.IOException;
import java.io.Writer;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * An single threaded alternative to the {@link java.io.StringWriter native
 * StringWriter} that is more efficient because it uses a {@link StringBuilder}.
 * 
 * @author Jeff Nelson
 */
@NotThreadSafe
public class StringBuilderWriter extends Writer {

    /**
     * Construct a new instance that writes to the specified
     * {@link StringBuilder builder}.
     * 
     * @param builder the destination for this writer
     */
    public StringBuilderWriter(StringBuilder builder) {
        this.sb = builder;
    }

    /**
     * Construct a new instance that writes to a new {@link StringBuilder}.
     */
    public StringBuilderWriter() {
        this(new StringBuilder());
    }

    /**
     * The {@link StringBuilder} to where this Writer sends characters.
     */
    private final StringBuilder sb;

    @Override
    public Writer append(char c) throws IOException {
        sb.append(c);
        return this;
    }

    @Override
    public Writer append(CharSequence csq) throws IOException {
        sb.append(csq);
        return this;
    }

    @Override
    public Writer append(CharSequence csq, int start, int end)
            throws IOException {
        sb.append(csq, start, end);
        return this;
    }

    @Override
    public void close() throws IOException {/* noop */}

    @Override
    public void flush() throws IOException {/* noop */}

    @Override
    public void write(char[] cbuf) throws IOException {
        sb.append(cbuf);
    }

    @Override
    public void write(char[] cbuf, int off, int len) throws IOException {
        sb.append(cbuf, off, len);

    }

    @Override
    public void write(int c) throws IOException {
        sb.append((char) c);
    }

    @Override
    public void write(String str) throws IOException {
        sb.append(str);
    }

    @Override
    public void write(String str, int off, int len) throws IOException {
        sb.append(str, off, len);
    }

    @Override
    public String toString() {
        return sb.toString();
    }

}
