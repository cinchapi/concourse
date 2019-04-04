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

import com.google.common.base.Preconditions;

/**
 * A {@link Page} is an object that is used to encapsulate the semantics of
 * a Page. Any any given time, objects of this class can exist in one
 * of two modes: {@code building} or {@code built}. When a Page is
 * {@code built}, it is guaranteed to represent a fully and well formed page
 * that can be processed. On the other hand, when a Page is {@code building}
 * it is in an incomplete state.
 * <p>
 * This class is the public interface to Page construction. It is meant to
 * be used in a chained manner, where the caller initially calls
 * {@link Page#number()} and continues to construct the Page using the
 * options available from each subsequently returned state.
 * </p>
 */
public class Page {

    /**
     * Start building a new {@link Page}.
     *
     * @return the Page builder
     */
    public static PageNumberState number(int number) {
        Page page = new Page();
        page.setNumber(number);
        return new PageNumberState(page);
    }

    /**
     * A flag that indicates whether this {@link Page} has been built.
     */
    private boolean built = false;

    /**
     * The page skip
     */
    private int skip;

    /**
     * The page limit
     */
    private int limit;

    /**
     * Construct a new instance.
     */
    protected Page() {
        this.skip = 0;
        this.limit = 0;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Page) {
            return ((Page) obj).skip == skip && ((Page) obj).limit == limit;
        }
        else {
            return false;
        }
    }

    /**
     * Returns the page number
     *
     * @return the page number
     */
    public int number() {
        return skip;
    }

    /**
     * Returns the page limit
     *
     * @return the page limit
     */
    public int container() {
        return limit;
    }

    /**
     * Sets the page number/skip
     *
     * @param number
     */
    public void setNumber(int number) {
        Preconditions.checkState(!built, "Cannot set number for a built page");
        this.skip = number;
    }

    /**
     * Sets the page container/offset
     *
     * @param container
     */
    public void setContainer(int container) {
        Preconditions.checkState(!built,
                "Cannot set container for a built page");
        this.limit = container;
    }

    /**
     * Mark this {@link Page} as {@code built}.
     */
    protected void close() {
        built = !built ? true : built;
    }
}
