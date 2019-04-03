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
package com.cinchapi.concourse.order;

/**
 * Represents a sort order on a key.
 */
public class SortOrder {

    /**
     * The key to sort on
     */
    private String key;

    /**
     * The order to sort in
     */
    private SortOrderType sortOrderType;

    /**
     * Constructs an instance
     *
     * @param key
     * @param sortOrderType
     */
    public SortOrder(String key, SortOrderType sortOrderType) {
        this.key = key;
        this.sortOrderType = sortOrderType;
    }

    /**
     * Constructs an instance with a default Ascending order
     * @param key
     */
    public SortOrder(String key) {
        this.key = key;
        this.sortOrderType = SortOrderType.ASCENDING;
    }

    /**
     * Changes the sort order to descending
     */
    public void descending() {
        sortOrderType = SortOrderType.DESCENDING;
    }

    /**
     * CHanges the sort order to ascending
     */
    public void ascending() {
        sortOrderType = SortOrderType.ASCENDING;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj.getClass() == getClass()) {
            return ((SortOrder) obj).key == key &&
                    ((SortOrder) obj).sortOrderType == sortOrderType;
        }
        else {
            return false;
        }
    }
}
