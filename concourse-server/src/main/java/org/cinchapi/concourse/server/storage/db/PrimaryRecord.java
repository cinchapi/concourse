/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2014 Jeff Nelson, Cinchapi Software Collective
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.cinchapi.concourse.server.storage.db;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import org.cinchapi.concourse.annotate.DoNotInvoke;
import org.cinchapi.concourse.annotate.PackagePrivate;
import org.cinchapi.concourse.server.model.PrimaryKey;
import org.cinchapi.concourse.server.model.Text;
import org.cinchapi.concourse.server.model.Value;
import org.cinchapi.concourse.server.storage.Versioned;

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
 * @author jnelson
 */
@ThreadSafe
@PackagePrivate
final class PrimaryRecord extends Record<PrimaryKey, Text, Value> {

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
            List<Revision<PrimaryKey, Text, Value>> revisions = history
                    .get(key); /* Authorized */
            if(revisions != null) {
                Iterator<Revision<PrimaryKey, Text, Value>> it = revisions
                        .iterator();
                while (it.hasNext()) {
                    Revision<PrimaryKey, Text, Value> revision = it.next();
                    audit.put(revision.getVersion(), revision.toString());
                }
            }
            return audit;
        }
        finally {
            read.unlock();
        }
    }

    /**
     * Return the Set of {@code keys} that map to fields which
     * <em>currently</em> contain values.
     * 
     * @return the Set of non-empty field keys
     */
    public Set<Text> describe() {
        return describe(false, Versioned.NO_VERSION);
    }

    /**
     * Return the Set of {@code keys} that mapped to fields which contained
     * values at {@code timestamp}.
     * 
     * @param timestamp
     * @return the Set of non-empty field keys
     */
    public Set<Text> describe(long timestamp) {
        return describe(true, timestamp);
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
     * Return an unmodifiable view of the Set of {@code keys} that
     * <em>currently</em> contain values or contained values at
     * {@code timestamp} if {@code historical} is {@code true}.
     * 
     * @param historical - if {@code true}, read from the history, otherwise
     *            read from the present state
     * @param timestamp - this value is ignored if {@code historical} is set to
     *            false, otherwise this value is the historical timestamp at
     *            which to read
     * @return the Set of non-empty field keys
     */
    private Set<Text> describe(boolean historical, long timestamp) {
        read.lock();
        try {
            if(historical) {
                Set<Text> description = Sets.newLinkedHashSet();
                Iterator<Text> it = history.keySet().iterator(); /* Authorized */
                while (it.hasNext()) {
                    Text key = it.next();
                    if(!get(key, timestamp).isEmpty()) {
                        description.add(key);
                    }
                }
                return description;
            }
            else {
                return Collections.unmodifiableSet(present.keySet()); /* Authorized */
            }
        }
        finally {
            read.unlock();
        }
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
