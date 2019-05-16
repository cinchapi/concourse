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
package com.cinchapi.concourse.lang.pagination;

import java.util.Objects;

import javax.annotation.concurrent.Immutable;

import com.cinchapi.common.base.Verify;

/**
 * A {@link Page} encapsulates the limit and skip/offset parameters that can be
 * used to pagination through a result set.
 */
@Immutable
public final class Page {

    /**
     * The default pagination size.
     */
    public final static int DEFAULT_SIZE = 20;

    /**
     * Return the {@link Page} at the specified {@code number} with
     * {@value #DEFAULT_SIZE} items.
     * 
     * @param number
     * @return the requested {@link Page}
     */
    public static Page at(int number) {
        return new Page(number, DEFAULT_SIZE);
    }

    /**
     * Return the first {@link Page} with {@value #DEFAULT_SIZE} items.
     * 
     * @return the first {@link Page}
     */
    public static Page first() {
        return at(1);
    }

    /**
     * Return the first {@link Page} with {@code size} items.
     * 
     * @param size
     * @return the first {@link Page}
     */
    public static Page with(int size) {
        return at(1).size(size);
    }

    /**
     * The number of this {@link Page} in a sequence.
     */
    private final int number;

    /**
     * The max number of elements on this {@link Page}.
     */
    private final int size;

    /**
     * Construct a new instance.
     * 
     * @param number
     * @param size
     */
    private Page(int number, int size) {
        Verify.thatArgument(number > 0, "Page must be greater than 0");
        Verify.thatArgument(size > 0, "Page must have a size greater than 0");
        this.number = number;
        this.size = size;
    }

    /**
     * Return the "previous" pagination.
     * <p>
     * This method returns a new {@link Page} whose skip/offset parameter is
     * reduced by this pagination's limit.
     * </p>
     * 
     * @return the next Page
     */
    public Page back() {
        return new Page(number - 1, size);
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Page) {
            return ((Page) obj).limit() == limit()
                    && ((Page) obj).skip() == skip();
        }
        else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(limit(), skip());
    }

    /**
     * Return the "next" pagination.
     * <p>
     * This method returns a new {@link Page} whose skip/offset parameter is
     * advanced by this pagination's limit.
     * </p>
     * 
     * @return the next Page
     */
    public Page next() {
        return new Page(number + 1, size);
    }

    /**
     * Set the size of this {@link Page}.
     * 
     * @param size
     * @return the re-sized {@link Page}
     */
    public Page size(int size) {
        return new Page(number, size);
    }

    /**
     * Go {@code to} the specified pagination.
     * 
     * @return the requested Page
     */
    public Page to(int number) {
        return new Page(number, size);
    }

    public int limit() {
        return size;
    }

    public int skip() {
        return (number - 1) * size;
    }
}
