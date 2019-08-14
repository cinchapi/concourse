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
package com.cinchapi.concourse.exporter;

import java.io.OutputStream;
import java.util.Map;
import java.util.Set;

import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.common.collect.Sequences;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * An {@link Exporter} for delimited lines (e.g. CSV).
 *
 * @author Jeff Nelson
 */
final class DelimitedLinesExporter<V> extends Exporter<V> {

    /**
     * Return the ideal string representation for {@code value}.
     * <p>
     * If {@code value} is a {@link Sequences#isSequence(Object) sequence}, this
     * method will return each item in the sequence as a comma separated string.
     * </p>
     * 
     * @param value
     * @return the ideal string representation for {@code value}
     */
    private static String stringify(Object value) {
        if(value == null) {
            return "null";
        }
        if(Sequences.isSequence(value)) {
            return AnyStrings.join(',', Sequences.stream(value).toArray());
        }
        else {
            return value.toString();
        }

    }

    /**
     * The delimiter to use.
     */
    private final char delimiter;

    /**
     * Construct a new instance.
     * 
     * @param data
     * @param output
     */
    protected DelimitedLinesExporter(OutputStream output, char delimiter) {
        super(output);
        this.delimiter = delimiter;
    }

    /**
     * {@inheritDoc}
     * <p>
     * /**
     * Output the {@code data} as a sequence of lines (one line per item) where
     * each key/value pair is separated by a {@code delimiter}. Conceptually,
     * this method can be used to transform a collection of {@link Map Maps} to
     * a CSV file.
     * </p>
     * <p>
     * The keys of all the items are joined to form a header line. If an
     * item does not contain a value for a header key, {@code null} is output
     * instead.
     * </p>
     * 
     * @param data
     */
    @Override
    public void export(Iterable<Map<String, V>> data) {
        Set<String> headers = Sets.newLinkedHashSet();
        for (Map<String, V> item : data) {
            for (String key : item.keySet()) {
                headers.add(key);
            }
        }
        print(headers, delimiter);
        for (Map<String, V> item : data) {
            output.println();
            Iterable<V> values = Iterables.transform(headers,
                    header -> item.get(header));
            print(values, delimiter);
        }

    }

    /**
     * Output the {@code values} to the {@code output} where each one is
     * separated by a {@code delimiter}.
     * 
     * @param values
     * @param delimiter
     * @param output
     */
    private void print(Iterable<?> values, char delimiter) {
        boolean prependComma = false;
        for (Object value : values) {
            if(prependComma) {
                output.print(',');
            }
            String $value = stringify(value);
            output.print(
                    AnyStrings.ensureWithinQuotesIfNeeded($value, delimiter));
            prependComma = true;
        }
        output.flush();

    }

}
