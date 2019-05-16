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

import com.cinchapi.concourse.thrift.TPage;

/**
 * Tools for translating aspects of the languages
 */
public class PageLanguage {

    /**
     * Translate the {@code page} to its Thrift analog.
     *
     * @param page
     * @return the analogous TPage
     */
    public static TPage translateToThriftPage(Page page) {
        return new TPage(page.skip() / page.limit() + 1, page.limit());
    }

    /**
     * Translate the {@code tpage} to its Java analog.
     *
     * @param tpage
     * @return the analogous Java {@link Page}
     */
    public static Page translateFromThriftPage(TPage tpage) {
        return Page.with(tpage.getSize()).to(tpage.getNumber());
    }
}
