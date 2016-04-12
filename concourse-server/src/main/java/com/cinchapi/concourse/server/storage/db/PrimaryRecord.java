/*
 * Copyright (c) 2013-2016 Cinchapi Inc.
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
package com.cinchapi.concourse.server.storage.db;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.cinchapi.concourse.annotate.DoNotInvoke;
import com.cinchapi.concourse.annotate.PackagePrivate;
import com.cinchapi.concourse.server.model.PrimaryKey;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.server.storage.Versioned;
import com.google.common.collect.Maps;

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
@PackagePrivate
final class PrimaryRecord extends BrowsableRecord<PrimaryKey, Text, Value> {

    /**
     * DO NOT INVOKE. Use {@link Record#createPrimaryRecord(PrimaryKey)} or
     * {@link Record#createPrimaryRecordPartial(PrimaryKey, Text)} instead.
     * 
     * @param locator
     * @param key
     */
    @PackagePrivate
    @DoNotInvoke
    protected PrimaryRecord(PrimaryKey locator, @Nullable Text key) {
        super(locator, key);
    }

    /**
     * Return a log of revision to the entire Record.
     * 
     * @return the revision log
     */
    public Map<Long, String> audit() {
        read.lock();
        try {
            Map<Long, String> audit = Maps.newTreeMap();
            for (Text key : present.keySet()) { /* Authorized */
                audit.putAll(audit(key));
            }
            return audit;
        }
        finally {
            read.unlock();
        }
    }

    /**
     * Return a log of revisions to the field mapped from {@code key}.
     * 
     * @param key
     * @return the revision log
     */
    public Map<Long, String> audit(Text key) {
        read.lock();
        try {
            Map<Long, String> audit = Maps.newLinkedHashMap();
            List<CompactRevision<Value>> revisions = history.get(key); /* Authorized */
            if(revisions != null) {
                Iterator<CompactRevision<Value>> it = revisions.iterator();
                while (it.hasNext()) {
                    CompactRevision<Value> revision = it.next();
                    audit.put(revision.getVersion(),
                            revision.toString(locator, key));
                }
            }
            return audit;
        }
        finally {
            read.unlock();
        }
    }

    /**
     * Return the Set of values <em>currently</em> contained in the field mapped
     * from {@code key}.
     * 
     * @param key
     * @return the Set of contained values
     */
    public Set<Value> fetch(Text key) {
        return fetch(key, false, Versioned.NO_VERSION);
    }

    /**
     * Return the Set of values contained in the field mapped from {@code key}
     * at {@code timestamp}.
     * 
     * @param key
     * @param timestamp
     * @return the Set of contained values
     */
    public Set<Value> fetch(Text key, long timestamp) {
        return fetch(key, true, timestamp);
    }

    /**
     * Return {@code true} if the Record <em>currently</em> contains data.
     * 
     * @return {@code true} if {@link #describe()} is not an empty Set
     */
    public boolean ping() {
        return !describe().isEmpty();
    }
    
    /**
     * Return {@code true} if {@code value} <em>currently</em> exists in the
     * field mapped from {@code key}.
     * 
     * @param key
     * @param value
     * @return {@code true} if {@code key} as {@code value} is a valid mapping
     */
    public boolean verify(Text key, Value value) {
        return verify(key, value, false, Versioned.NO_VERSION);
    }

    /**
     * Return {@code true} if {@code value} existed in the field mapped from
     * {@code key} at {@code timestamp}
     * 
     * @param key
     * @param value
     * @param timestamp
     * @return {@code true} if {@code key} as {@code value} is a valid mapping
     */
    public boolean verify(Text key, Value value, long timestamp) {
        return verify(key, value, true, timestamp);
    }

    @Override
    protected Map<Text, Set<Value>> mapType() {
        return Maps.newHashMap();
    }

    /**
     * Return an unmodifiable view of the Set of values <em>currently</em>
     * contained in the field mapped from {@code key} or contained at
     * {@code timestamp} if {@code historical} is {@code true}.
     * 
     * @param key
     * @param historical - if {@code true}, read from the history, otherwise
     *            read from the present state
     * @param timestamp - this value is ignored if {@code historical} is set to
     *            false, otherwise this value is the historical timestamp at
     *            which to read
     * @return the Set of contained values
     */
    private Set<Value> fetch(Text key, boolean historical, long timestamp) {
        // NOTE: locking happens in super.get() methods
        return historical ? get(key, timestamp) : get(key);
    }

    /**
     * Return {@code true} if {@code value} <em>currently</em> exists in the
     * field mapped from {@code key} or existed in that field at
     * {@code timestamp} if {@code historical} is {@code true}.
     * 
     * @param key
     * @param value
     * @param historical - if {@code true}, read from the history, otherwise
     *            read from the present state
     * @param timestamp - this value is ignored if {@code historical} is set to
     *            false, otherwise this value is the historical timestamp at
     *            which to read
     * @return {@code true} if {@code key} as {@code value} is a valid mapping
     */
    private boolean verify(Text key, Value value, boolean historical,
            long timestamp) {
        // NOTE: locking happens in super.get() methods
        return historical ? get(key, timestamp).contains(value) : get(key)
                .contains(value);
    }

}
