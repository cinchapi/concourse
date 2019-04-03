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
package com.cinchapi.concourse.page;

/**
 * The base class for a Page state that can be transformed into a complete
 * and well-formed {@link Page}.
 */
public abstract class BuildableState extends State {

    /**
     * Construct a new instance.
     *
     * @param page
     */
    protected BuildableState(Page page) {
        super(page);
    }

    /**
     * Build and return the {@link Page}.
     *
     * @return the built Page
     */
    public final Page build() {
        page.close();
        return page;
    }
}
