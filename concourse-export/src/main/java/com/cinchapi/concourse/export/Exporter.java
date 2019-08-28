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
package com.cinchapi.concourse.export;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Map;

import javax.annotation.concurrent.Immutable;

/**
 * An {@link Exporter} can push objects (represented as a Map} to an
 * {@link OutputStream}.
 *
 * @author Jeff Nelson
 */
@Immutable
public abstract class Exporter<V> {

    /**
     * The place to which the {@code data} is exported.
     */
    protected final PrintStream output;

    /**
     * Construct a new instance.
     * 
     * @param output
     */
    public Exporter(OutputStream output) {
        this.output = output instanceof PrintStream ? (PrintStream) output
                : new PrintStream(output);
    }

    /**
     * Export each item in the {@code data} to the {@code output} stream using
     * the rules of this {@link Exporter}.
     * 
     * @param data
     */
    public abstract void export(Iterable<Map<String, V>> data);

    @Override
    protected void finalize() throws Throwable {
        output.close();
        super.finalize();
    }

}
