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

/**
 * This is the base class and marker for any valid state in the {@link Page}
 * builder. Each {@link State} is passed the current {@link Page} and holds
 * a reference.
 * <p>
 * For the purposes of a builder, a {@link State} typically describes what was
 * most recently consumed.
 * </p>
 */
public abstract class State {

    /**
     * A reference to the {@link Page} that is being built.
     */
    protected final Page page;

    /**
     * Construct a new instance.
     *
     * @param page
     */
    protected State(Page page) {
        this.page = page;
    }

}
