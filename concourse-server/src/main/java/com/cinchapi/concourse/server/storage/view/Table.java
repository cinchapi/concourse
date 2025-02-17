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
package com.cinchapi.concourse.server.storage.view;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import com.cinchapi.concourse.server.storage.Action;
import com.cinchapi.concourse.server.storage.temp.Write;
import com.cinchapi.concourse.thrift.TObject;

/**
 * A structured data view that organizes {@link Write Writes} into a tabular
 * format.
 * 
 * Each cell (intersection of row and column) can hold multiple values,
 * stored in insertion order to preserve data consistency.
 *
 * This class is designed to act as a lightweight data store that allows
 * efficient retrieval, modification, and deletion of data while ensuring
 * that empty mappings are removed automatically.
 * 
 * @author Jeff Nelson
 */
public class Table {

    private final Map<Long, Map<String, Set<TObject>>> data;

    /**
     * Construct a new instance.
     */
    public Table() {
        this.data = new HashMap<>();
    }

    /**
     * Associates a value with the specified record and key intersection.
     * 
     * @param record The record identifier
     * @param key The key identifier
     * @param value The value to associate
     * @return {@code true} if the value was newly added, {@code false} if
     *         it was
     *         already present
     * @throws NullPointerException if any parameter is null
     */
    public boolean add(Long record, String key, TObject value) {
        Objects.requireNonNull(record, "Record cannot be null");
        Objects.requireNonNull(key, "Key cannot be null");
        Objects.requireNonNull(value, "Value cannot be null");

        return data.computeIfAbsent(record, k -> new HashMap<>())
                .computeIfAbsent(key, k -> new LinkedHashSet<>()).add(value);
    }

    /**
     * Removes all mappings from the table.
     */
    public void clear() {
        data.clear();
    }

    /**
     * Removes and returns all values associated with the specified record
     * and key
     * intersection.
     * 
     * @param record The record identifier
     * @param key The key identifier
     * @return An unmodifiable set of the removed values, or an empty set if
     *         the
     *         cell was empty
     * @throws NullPointerException if any parameter is null
     */
    public Set<TObject> clear(Long record, String key) {
        Objects.requireNonNull(record, "Record cannot be null");
        Objects.requireNonNull(key, "Key cannot be null");

        Map<String, Set<TObject>> recordMap = data.get(record);
        if(recordMap == null) {
            return Collections.emptySet();
        }

        Set<TObject> removed = recordMap.remove(key);
        if(recordMap.isEmpty()) {
            data.remove(record);
        }
        return removed != null ? Collections.unmodifiableSet(removed)
                : Collections.emptySet();
    }

    /**
     * Removes and returns all key mappings for a specific record.
     * 
     * @param record The record identifier
     * @return An unmodifiable map of the removed key mappings and their
     *         values, or an empty map if the record didn't exist
     * @throws NullPointerException if record is null
     */
    public Map<String, Set<TObject>> delete(Long record) {
        Objects.requireNonNull(record, "Record cannot be null");
        Map<String, Set<TObject>> removed = data.remove(record);
        if(removed == null) {
            return Collections.emptyMap();
        }

        // Create unmodifiable view with unmodifiable sets
        Map<String, Set<TObject>> unmodifiableMap = new HashMap<>();
        removed.forEach((key, value) -> unmodifiableMap.put(key,
                Collections.unmodifiableSet(value)));
        return Collections.unmodifiableMap(unmodifiableMap);
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Table) {
            return data.equals(((Table) obj).data);
        }
        else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return data.hashCode();
    }

    /**
     * Checks if the table contains any mappings.
     * 
     * @return {@code true} if the table has no mappings, {@code false}
     *         otherwise
     */
    public boolean isEmpty() {
        return data.isEmpty();
    }

    /**
     * Retrieves all values associated with the specified record and key
     * intersection.
     * 
     * @param record The record identifier
     * @param key The key identifier
     * @return An unmodifiable view of the values at the specified cell, or
     *         an
     *         empty set if the cell doesn't exist
     * @throws NullPointerException if any parameter is null
     */
    public Set<TObject> lookup(Long record, String key) {
        Objects.requireNonNull(record, "Record cannot be null");
        Objects.requireNonNull(key, "Key cannot be null");

        return Collections.unmodifiableSet(
                data.getOrDefault(record, Collections.emptyMap())
                        .getOrDefault(key, Collections.emptySet()));
    }

    /**
     * Processes a write operation by either adding or removing the
     * specified
     * record based on the write action type.
     *
     * @param write The write operation to process
     * @throws NullPointerException if write is null
     */
    public void put(Write write) {
        Objects.requireNonNull(write, "Write cannot be null");
        long record = write.getRecord().longValue();
        String key = write.getKey().toString();
        TObject value = write.getValue().getTObject();
        if(write.getType() == Action.REMOVE) {
            remove(record, key, value);
        }
        else {
            add(record, key, value);
        }
    }

    /**
     * Dissociates a value from the specified record and key intersection.
     * 
     * @param record The record identifier
     * @param key The key identifier
     * @param value The value to dissociate
     * @return {@code true} if the value was removed, {@code false} if it
     *         wasn't
     *         present
     * @throws NullPointerException if any parameter is null
     */
    public boolean remove(Long record, String key, TObject value) {
        Objects.requireNonNull(record, "Record cannot be null");
        Objects.requireNonNull(key, "Key cannot be null");
        Objects.requireNonNull(value, "Value cannot be null");

        return data.computeIfPresent(record, (recordKey, recordMap) -> {
            boolean removed = Optional.ofNullable(recordMap.get(key))
                    .map(values -> values.remove(value)).orElse(false);

            if(removed) {
                if(recordMap.get(key).isEmpty()) {
                    recordMap.remove(key);
                }
                return recordMap.isEmpty() ? null : recordMap;
            }
            return recordMap;
        }) != null;
    }

    /**
     * Returns all record identifiers present in the table.
     * 
     * @return An unmodifiable view of all record identifiers
     */
    public Set<Long> rows() {
        return Collections.unmodifiableSet(data.keySet());
    }

    /**
     * Retrieves all key mappings for a specific record.
     * 
     * @param record The record identifier
     * @return An unmodifiable view of the record's key to values mappings,
     *         or an
     *         empty map if the record doesn't exist
     * @throws NullPointerException if record is null
     */
    public Map<String, Set<TObject>> select(Long record) {
        Objects.requireNonNull(record, "Record cannot be null");

        return Collections.unmodifiableMap(data
                .getOrDefault(record, Collections.emptyMap()).entrySet()
                .stream().collect(HashMap::new,
                        (m, e) -> m.put(e.getKey(),
                                Collections.unmodifiableSet(e.getValue())),
                        HashMap::putAll));
    }

    /**
     * Returns the number of records in the table.
     * 
     * @return The count of records
     */
    public int size() {
        return data.size();
    }

    @Override
    public String toString() {
        return data.toString();
    }

}