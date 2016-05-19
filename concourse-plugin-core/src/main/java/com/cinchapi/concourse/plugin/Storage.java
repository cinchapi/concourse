/*
 * Copyright (c) 2013-2016 Cinchapi Inc.
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
package com.cinchapi.concourse.plugin;

import java.util.Map;
import java.util.Set;

import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.thrift.TObject;

/**
 * The {@code Storage} API abstracts the location where the
 * {@link ConcourseRuntime} stores data and provides primitive low-level methods
 * that allow a plugin to directly read from or write to the data files.
 * 
 * @author Jeff Nelson
 */
public interface Storage {

    /**
     * Add data to a field if it doesn't already exist.
     * 
     * @param key - The field name
     * @param value - The value to add
     * @param record - The record that contains the field
     * @return {@code true} if the value is added to the field, {@code false}
     *         otherwise
     */
    public boolean add(String key, TObject value, long record);

    /**
     * Return a log of all the revisions made to a {@code record}.
     * 
     * @param record - The record to audit
     * @return A mapping from the timestamp to a description of each revision
     */
    public Map<Long, String> audit(long record);

    /**
     * Return a log of all the revisions made to a field.
     * 
     * @param key - The field name
     * @param record - The record that contains the field
     * @return A mapping from the timestamp to a description of each revision
     */
    public Map<Long, String> audit(String key, long record);

    /**
     * Return a view of the index for {@code key}.
     * 
     * @param key - The index name
     * @return A mapping from the indexed value to a Set of all the records that
     *         contain the value in the {@code key} field
     */
    public Map<TObject, Set<Long>> browse(String key);

    /**
     * Return a view of the index for {@code key} at {@code timestamp}.
     * 
     * @param key - The index name
     * @param timestamp - The unix timestamp to check
     * @return A mapping from the indexed value to a Set of all the records that
     *         contained the value in the {@code key} field at {@code timestamp}
     */
    public Map<TObject, Set<Long>> browse(String key, long timestamp);

    /**
     * Return {@code true} if data has ever existed in {@code record}.
     * 
     * @param record - The record id
     * @return {@code true} if data has ever existed in {@code record},
     *         {@code false} otherwise
     */
    public boolean contains(long record);

    /**
     * Return a Set that contains all the field names in {@code record}.
     * 
     * @param record - The record id
     * @return A set that contains all the field names
     */
    public Set<String> describe(long record);

    /**
     * Return a Set that contains all the field names in {@code record} at
     * {@code timestamp}.
     * 
     * @param record - The record id
     * @param timestamp - The unix timestamp to check
     * @return A set that contains all the field names at {@code timestamp}
     */
    public Set<String> describe(long record, long timestamp);

    /**
     * Return a view of all the records and the matching values that satisfied a
     * criteria at {@code timestamp}.
     * 
     * @param timestamp - The unix timestamp to use
     * @param key - The criteria index name
     * @param operator - The criteria operator
     * @param values - The values to use in relation to the {@code operator} to
     *            determine matches
     * @return A mapping from each record to the Set of matching values that
     *         satisfied the criteria at {@code timestamp}
     */
    public Map<Long, Set<TObject>> explore(long timestamp, String key,
            Operator operator, TObject... values);

    /**
     * Return a view of all the records and the matching values that satisfy a
     * criteria.
     * 
     * @param key - The criteria index name
     * @param operator - The criteria operator
     * @param values - The values to use in relation to the {@code operator} to
     *            determine matches
     * @return A mapping from each record to the Set of matching values that
     *         satisfy the criteria
     */
    public Map<Long, Set<TObject>> explore(String key, Operator operator,
            TObject... values);

    /**
     * Find the records that have values in the {@code key} field that matched a
     * criteria at {@code timestamp}.
     * 
     * @param timestamp - The unix timestamp to use
     * @param key - The criteria index name
     * @param operator - The criteria operator
     * @param values - The values to use in relation to the {@code operator} to
     *            determine matches
     * @return A Set of records that have values in the {@code key} field that
     *         satisfied the criteria at {@code timestamp}.
     */
    public Set<Long> find(long timestamp, String key, Operator operator,
            TObject... values);

    /**
     * Find the records that have values in the {@code key} field that match a
     * criteria.
     * 
     * @param key - The criteria index name
     * @param operator - The criteria operator
     * @param values - The values to use in relation to the {@code operator} to
     *            determine matches
     * @return A Set of records that have values in the {@code key} field that
     *         satisfy the criteria.
     */
    public Set<Long> find(String key, Operator operator, TObject... values);

    /**
     * Remove data from a field if exist.
     * 
     * @param key - The field name
     * @param value - The value to remove
     * @param record - The record that contains the field
     * @return {@code true} if the value is removed from the field,
     *         {@code false} otherwise.
     */
    public boolean remove(String key, TObject value, long record);

    /**
     * Perform a full text search on an index for records that have values
     * matching a {@code query}.
     * 
     * @param key - The index name
     * @param query - The query to use in the search
     * @return A Set of records that have values in the {@code key} field that
     *         match the search {@code query}
     */
    public Set<Long> search(String key, String query);

    /**
     * Select all of the data from {@code record}.
     * 
     * @param record - The record id
     * @return A mapping from each field name to the Set of values contained in
     *         the field
     */
    public Map<String, Set<TObject>> select(long record);

    /**
     * Select all of the data from {@code record} at {@code timestamp}.
     * 
     * @param record - The record id
     * @param timestamp - The unix timestamp to use
     * @return A mapping from each field name to the Set of values contained in
     *         the field at {@code timestamp}
     */
    public Map<String, Set<TObject>> select(long record, long timestamp);

    /**
     * Select all of the data from a field.
     * 
     * @param key - The field name
     * @param record - The record that contains the field
     * @return The Set of values contained in the field
     */
    public Set<TObject> select(String key, long record);

    /**
     * Select all of the data from a field at {@code timestamp}.
     * 
     * @param key - The field name
     * @param record - The record that contains the field
     * @param timestamp - The unix timestamp to use
     * @return The Set of values contained in the field at {@code timestamp}
     */
    public Set<TObject> select(String key, long record, long timestamp);

    /**
     * Atomically remove all the data from a field and add {@code value} so that
     * it is the only one contained.
     * 
     * @param key - The field name
     * @param value - The value to set
     * @param record - The record that contains the field.
     */
    public void set(String key, TObject value, long record);

    /**
     * Return {@code true} if the {@code key} field in {@code record} contains
     * {@code value}.
     * 
     * @param key - The field name
     * @param value - The value to verify
     * @param record - The record that contains the field
     * @return {@code true} if {@code value} is contained within the field,
     *         {@code false} otherwise
     */
    public boolean verify(String key, TObject value, long record);

    /**
     * Return {@code true} if the {@code key} field in {@code record} contained
     * {@code value} at {@code timestamp}.
     * 
     * @param key - The field name
     * @param value - The value to verify
     * @param record - The record that contains the field
     * @param timestamp - The unix timestamp to use
     * @return {@code true} if {@code value} was contained within the field at
     *         {@code timestamp}, {@code false} otherwise
     */
    public boolean verify(String key, TObject value, long record, long timestamp);

}
