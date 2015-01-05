/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2015 Jeff Nelson, Cinchapi Software Collective
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
package org.cinchapi.concourse;

import javax.annotation.concurrent.Immutable;

import com.google.common.base.Strings;
import com.google.common.collect.ComparisonChain;

/**
 * A {@link Tag} is a {@link String} data type that does not get full-text
 * indexed.
 * <p>
 * Each Tag is equivalent to its String counterpart (e.g.
 * {@code Tag.create("foo").equals(new String("foo"))} is {@code true}. Tags
 * merely exist for the client to instruct Concourse not to full text index the
 * data. Tags are stored as strings within Concourse. And any value that is
 * written as a Tag is always returned as a String when reading from Concourse.
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
