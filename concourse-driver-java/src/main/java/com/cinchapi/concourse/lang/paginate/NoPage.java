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

/**
 * A specification for no {@link Page}.
 *
 * @author Jeff Nelson
 */
public class NoPage implements Page {

    /**
     * Singleton instance.
     */
    public static NoPage INSTANCE = new NoPage();

    /**
     * Construct a new instance.
     */
    private NoPage() {/* singleton */}

    @Override
    public Page back() {
        return this;
    }

    @Override
    public Page go(int page) {
        return this;
    }

    @Override
    public int limit() {
        return Integer.MAX_VALUE;
    }

    @Override
    public Page next() {
        return this;
    }

    @Override
    public int offset() {
        return 0;
    }

    @Override
    public Page resize(int size) {
        return this;
    }

    @Override
    public Page size(int size) {
        return this;
    }

}
