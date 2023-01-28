/*
 * Copyright (c) 2013-2023 Cinchapi Inc.
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

/**
 * A {@link Page} that is defined by a skip (e.g., offset) and a limit.
 *
 * @author Jeff Nelson
 */
@Immutable
final class SkipLimit implements Page {

    /**
     * The page limit
     */
    private final int limit;

    /**
     * The page offset.
     */
    private final int skip;

    /**
     * Construct a new instance.
     * 
     * @param skip
     * @param limit
     */
    SkipLimit(int skip, int limit) {
        this.skip = skip;
        this.limit = limit;
    }

    @Override
    public Page back() {
        if(skip > 0) {
            return new SkipLimit(Math.max(0, skip - limit), limit);
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

    @Override
    public Page go(int page) {
        return new SkipLimit((page - 1) * limit, limit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(limit(), skip());
    }

    @Override
    public int limit() {
        return limit;
    }

    @Override
    public Page next() {
        return new SkipLimit(skip + limit, limit);
    }

    @Override
    public int offset() {
        return skip;
    }

    @Override
    public Page resize(int size) {
        int page = skip / limit;
        return new SkipLimit(page * size, size);
    }

    @Override
    public Page size(int size) {
        return new SkipLimit(skip, size);
    }

}
