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
package org.cinchapi.concourse;

import javax.annotation.concurrent.Immutable;

import com.google.common.collect.ComparisonChain;

/**
 * A {@link Tag} is a wrapper around {@link String} that represents the
 * string value in key in record and distinguishes from simple String values. A
 * Tag does not get full-text indexed.
 * 
 * @author knd
 */
@Immutable
public final class Tag implements Comparable<Tag> {

    /**
     * Return a Tag that embeds {@code value}.
     * 
     * @param value
     * @return the Tag
     */
    public static Tag create(String value) {
        return new Tag(value);
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
