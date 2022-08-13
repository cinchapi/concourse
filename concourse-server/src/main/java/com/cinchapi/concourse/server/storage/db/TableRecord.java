/*
 * Copyright (c) 2013-2022 Cinchapi Inc.
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
package com.cinchapi.concourse.server.storage.db;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.cinchapi.concourse.annotate.DoNotInvoke;
import com.cinchapi.concourse.annotate.PackagePrivate;
import com.cinchapi.concourse.server.model.Identifier;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.server.storage.Action;
import com.cinchapi.concourse.time.Time;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * A logical grouping of data for a single entity.
 * <p>
 * This is the primary view of stored data within Concourse, similar to a Row in
 * a traditional database. PrimaryRecords are designed to efficiently handle
 * direct/non-query reads.
 * </p>
 * 
 * @author Jeff Nelson
 */
@ThreadSafe
public final class TableRecord extends Record<Identifier, Text, Value> {

    /**
     * Return a {@link TableRecord} that holds data for the {@code locator}.
     * 
     * @param locator
     * @return the {@link TableRecord}
     */
    public static TableRecord create(Identifier locator) {
        return new TableRecord(locator, null);
    }

    /**
     * Return a {@link TableRecord} that holds data for {@code key} in
     * {@code locator}.
     * 
     * @param locator
     * @param key
     * @return the {@link TableRecord}
     */
    public static TableRecord createPartial(Identifier locator, Text key) {
        return new TableRecord(locator, key);
    }

    /**
     * DO NOT INVOKE. Use {@link Record#createPrimaryRecord(Identifier)} or
     * {@link Record#createPrimaryRecordPartial(Identifier, Text)} instead.
     * 
     * @param locator
     * @param key
     */
    @PackagePrivate
    @DoNotInvoke
    protected TableRecord(Identifier locator, @Nullable Text key) {
        super(locator, key);
    }

    /**
     * Return a time series of values that holds the data stored for {@code key}
     * after each modification.
     * 
     * @param key the field name
     * @param start the start timestamp (inclusive)
     * @param end the end timestamp (exclusive)
     * @return the time series of values held in the {@code key} field between
     *         {@code start} and {@code end}
     */
    public Map<Long, Set<Value>> chronologize(Text key, long start, long end) {
        read.lock();
        try {
            Map<Long, Set<Value>> context = Maps.newLinkedHashMap();
            List<CompactRevision<Value>> revisions = history.get(key);
            Set<Value> snapshot = Sets.newLinkedHashSet();
            if(revisions != null) {
                Iterator<CompactRevision<Value>> it = revisions.iterator();
                while (it.hasNext()) {
                    CompactRevision<Value> revision = it.next();
                    long timestamp = revision.getVersion();
                    if(timestamp >= end) {
                        break;
                    }
                    else {
                        Action action = revision.getType();
                        snapshot = Sets.newLinkedHashSet(snapshot);
                        Value value = revision.getValue();
                        if(action == Action.ADD) {
                            snapshot.add(value);
                        }
                        else if(action == Action.REMOVE) {
                            snapshot.remove(value);
                        }
                        if(timestamp >= start) {
                            context.put(timestamp, snapshot);
                        }
                    }
                }
            }
            if(snapshot.isEmpty()) {
                // CON-474: If the last snapshot is empty, add it here so that
                // the Buffer has the proper context
                context.put(Time.NONE, snapshot);
            }
            return context;
        }
        finally {
            read.unlock();
        }
    }

    /**
     * Return a log of changes made to the entire Record.
     * 
     * @return the revision log
     */
    public Map<Long, List<String>> review() {
        read.lock();
        try {
            Map<Long, List<String>> review = new TreeMap<>();
            for (Entry<Text, List<CompactRevision<Value>>> entry : history
                    .entrySet()) {
                String key = entry.getKey().toString();
                for (CompactRevision<Value> revision : entry.getValue()) {
                    review.computeIfAbsent(revision.getVersion(),
                            $ -> new ArrayList<>())
                            .add(revision.toString(locator, key));
                }
            }
            return review;
        }
        finally {
            read.unlock();
        }
    }

    /**
     * Return a log of changes made to the field mapped from {@code key}.
     * 
     * @param key
     * @return the revision log
     */
    public Map<Long, List<String>> review(Text key) {
        read.lock();
        try {
            Map<Long, List<String>> review = new LinkedHashMap<>();
            List<CompactRevision<Value>> revisions = history
                    .get(key); /* Authorized */
            if(revisions != null) {
                Iterator<CompactRevision<Value>> it = revisions.iterator();
                while (it.hasNext()) {
                    CompactRevision<Value> revision = it.next();
                    review.computeIfAbsent(revision.getVersion(),
                            $ -> new ArrayList<>())
                            .add(revision.toString(locator, key));
                }
            }
            return review;
        }
        finally {
            read.unlock();
        }
    }

    @Override
    protected Map<Text, Set<Value>> $createDataMap() {
        return Maps.newHashMap();
    }
}
