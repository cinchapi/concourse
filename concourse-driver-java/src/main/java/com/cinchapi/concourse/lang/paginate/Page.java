/*
 * Copyright (c) 2013-2022 Cinchapi Inc.
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
package com.cinchapi.concourse.lang.paginate;

import java.util.Objects;

import javax.annotation.concurrent.Immutable;

import com.cinchapi.ccl.grammar.PageSymbol;
import com.cinchapi.ccl.syntax.PageTree;

/**
 * A {@link Page} contains a subset of data. The data on a {@link Page} is
 * determined by applying an {@link #offset() offset} and {@link #limit() limit}
 * to a data set with a consistent order.
 * <p>
 * Effectively, a {@link Page} encapsulates the limit and skip/offset parameters
 * that can be used to pagination through a result set.
 * </p>
 *
 * @author Jeff Nelson
 */
@Immutable
public final class Page {

    /**
     * Return the first {@link Page}
     * 
     * @return the first {@link Page}
     */
    public static Page first() {
        return new Page(DEFAULT_OFFSET, DEFAULT_LIMIT);
    }

    /**
     * Return a {@link Page} based on the parsed statement that produced the
     * {@link PageTree}.
     * 
     * @param tree
     * @return the associated {@link Page}
     */
    public static Page from(PageTree tree) {
        PageSymbol symbol = (PageSymbol) tree.root();
        return sized(symbol.size()).go(symbol.number());
    }

    /**
     * Return the {@link Page} at {@code number}.
     * 
     * @param number
     * @return the {@code number} Page
     */
    public static Page number(int number) {
        return first().go(number);
    }

    /**
     * Return a {@link Page} with explicit {@code skip} and {@code limit}
     * parameters.
     * 
     * @param skip
     * @param limit
     * @return the Page
     */
    public static Page of(int skip, int limit) {
        return new Page(skip, limit);
    }

    /**
     * Return the first {@link Page} with {@code size} items.
     * 
     * @param size
     * @return the first {@link Page} with {@code size} items
     */
    public static Page sized(int size) {
        return first().size(size);
    }

    /**
     * The default number of items to include on a Page.
     */
    static final int DEFAULT_LIMIT = 20;

    /**
     * The default number of items to skip before adding items to a Page.
     */
    static final int DEFAULT_OFFSET = 0;

    /**
     * The page limit
     */
    private final int limit;

    /**
     * The page offset.
     */
    private final int offset;

    /**
     * Construct a new instance.
     * 
     * @param offset
     * @param limit
     */
    private Page(int offset, int limit) {
        this.offset = offset;
        this.limit = limit;
    }

    /**
     * Return the "previous" {@link Page}, if possible. If this is already the
     * first {@link Page}, it is returned from this method.
     * 
     * @return the previous page
     */
    public Page back() {
        if(offset > 0) {
            return new Page(Math.max(0, offset - limit), limit);
        }
        else {
            return this;
        }
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

    /**
     * Jump to {@code page} {@link Page} while keeping the same limit.
     * 
     * @param page
     * @return the specified page
     */
    public Page go(int page) {
        return new Page((page - 1) * limit, limit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(limit(), skip());
    }

    /**
     * Return the maximum number of items on the page. The number of items
     * returned may be less than this value if there are fewer than
     * {@link #limit() limit} items remaining in the data set.
     * 
     * @return the limit
     */
    public int limit() {
        return limit;
    }

    /**
     * Return the "next" {@link Page}.
     * <p>
     * This method returns a new {@link Page} whose skip/offset parameter is
     * advanced by this pagination's limit.
     * </p>
     * 
     * @return the next Page
     */
    public Page next() {
        return new Page(offset + limit, limit);
    }

    /**
     * Return the number of items to "skip" (inclusive) before adding items to
     * the page.
     * 
     * @return the offset
     */
    public int offset() {
        return offset;
    }

    /**
     * Set this {@link Page} to include up to {@code size} items with the
     * assumption that all previous pages contained {@code size} items.
     * <p>
     * NOTE: This method recalculates all the items on the current page as if
     * all preceeding pages also had {@code size} items. If you want to modify
     * this page to include {@code size} without modifying the assumptions about
     * what was contained on previous pages, use the {@link #size(int)} method.
     * </p>
     * 
     * @param size
     * @return the configured Page
     */
    public Page resize(int size) {
        int page = offset / limit;
        return new Page(page * size, size);
    }

    /**
     * Set this {@link Page} to include up to {@code size} items.
     * <p>
     * NOTE: This method only modifies the current page size (e.g. adds/removes)
     * items instead of recalculating all the items on the current page as if
     * all preceeding pages also had {@code size} items. If you want to modify
     * this page to include {@code size} items in a manner that assumes
     * preceding pages also contained {@code size} items, use the
     * {@link #resize(int)} method.
     * </p>
     * 
     * @param size
     * @return the configured Page
     */
    public Page size(int size) {
        return new Page(offset, size);
    }

    /**
     * An alias for {@link #offset()}.
     * 
     * @return the offset
     */
    public int skip() {
        return offset();
    }

}
