/*
 * Copyright (c) 2013-2018 Cinchapi Inc.
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
package com.cinchapi.concourse;

import javax.annotation.concurrent.Immutable;

import com.google.common.base.Strings;
import com.google.common.collect.ComparisonChain;

/**
 * A {@link Tag} is a {@link String} data type that does not get indexed for
 * full text search.
 * <p>
 * Each Tag is equivalent to its String counterpart (e.g.
 * {@code Tag.create("foo").equals(new String("foo"))} is {@code true}. Tags
 * merely exist for the client to instruct Concourse not to perform full text
 * indexing on the data. Within Concourse, Tags are stored on disk as strings.
 * So, any value that is written as a Tag is always returned as a String when
 * read from Concourse.
 * </p>
 * 
 * @author knd
 */
@Immutable
public final class Tag implements Comparable<Tag> {

    /**
     * A singleton {@link Tag} that represents the empty string.
     */
    public static final Tag EMPTY_TAG = new Tag("");

    /**
     * Return a Tag that embeds {@code value}.
     * 
     * @param value
     * @return the Tag
     */
    public static Tag create(String value) {
        if(Strings.isNullOrEmpty(value)) {
            return EMPTY_TAG;
        }
        else {
            return new Tag(value);
        }
    }

    /**
     * The String representation for the value in key in record
     * that this Tag embeds.
     */
    private final String value;

    /**
     * Construct a new instance.
     * 
     * @param value
     */
    private Tag(String value) {
        this.value = value;
    }

    @Override
    public int compareTo(Tag other) {
        return ComparisonChain.start().compare(toString(), other.toString())
                .result();
    }

    /**
     * Return {@code true} if {@code other} of type String or
     * Tag equals this Tag.
     * 
     * @param other
     * @return {@code true} if {@code other} equals this tag
     */
    @Override
    public boolean equals(Object other) {
        boolean isEqual = false;
        if(other instanceof Tag) {
            isEqual = compareTo((Tag) other) == 0;
        }
        else if(other instanceof String) {
            isEqual = value.equals(other.toString());
        }
        return isEqual;
    }

    /**
     * Return the String value that this Tag embeds.
     * 
     * @return the value
     */
    @Override
    public String toString() {
        return value;
    }

}
