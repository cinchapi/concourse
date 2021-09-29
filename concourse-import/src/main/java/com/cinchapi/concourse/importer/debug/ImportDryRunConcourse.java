/*
 * Copyright (c) 2013-2021 Cinchapi Inc.
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
package com.cinchapi.concourse.importer.debug;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.NoOpConcourse;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.TSets;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

/**
 * A {@link Concourse} API that holds {@link #insert(String) inserted} data in
 * memory.
 * <p>
 * This particular API is designed to allow the import framework to perform
 * dry-run data imports.
 * </p>
 * <p>
 * <strong>NOTE:</strong> By default, this class intentionally contains
 * <em>static state.</em>. In order to support multi-threaded imports, this
 * class maintains a global view of data that has been inserted from multiple
 * instances.
 * </p>
 * 
 * @author Jeff Nelson
 */
public class ImportDryRunConcourse extends NoOpConcourse {

    /**
     * The imported data across ALL instances.
     */
    private static final List<Multimap<String, Object>> IMPORTED = Lists
            .newArrayList();

    /**
     * The list that holds each of the imported records, represented as a
     * multimap.
     */
    private final List<Multimap<String, Object>> imported;

    /**
     * Construct a new instance.
     */
    public ImportDryRunConcourse() {
        this(false);
    }

    /**
     * Construct a new instance.
     * 
     * @param isolated
     */
    public ImportDryRunConcourse(boolean isolated) {
        imported = isolated ? Lists.newArrayList() : IMPORTED;
    }

    @Override
    public Set<String> describe() {
        synchronized (imported) {
            Set<String> keys = Sets.newHashSet();
            imported.forEach(record -> keys.addAll(record.keySet()));
            return keys;
        }
    }

    /**
     * Dump the data that was inserted as a JSON blob.
     * 
     * @return a json dump
     */
    public String dump() {
        StringBuilder sb = new StringBuilder();
        synchronized (imported) {
            sb.append("[");
            for (Multimap<String, Object> map : imported) {
                sb.append(Convert.mapToJson(map)).append(",");
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.append("]");
            return sb.toString();
        }

    }

    @Override
    public void exit() {}

    @Override
    public Set<Long> insert(String json) {
        List<Multimap<String, Object>> data = Convert.anyJsonToJava(json);
        if(!data.isEmpty()) {
            synchronized (imported) {
                imported.addAll(data);
                long start = imported.size() + 1;
                long end = start + data.size() - 1;
                return TSets.sequence(start, end);
            }
        }
        else {
            return Collections.emptySet();
        }
    }

    @Override
    protected Concourse copyConnection() {
        return new ImportDryRunConcourse();
    }

}
