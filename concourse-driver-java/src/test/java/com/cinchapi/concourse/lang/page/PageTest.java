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
package com.cinchapi.concourse.lang.page;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link com.cinchapi.concourse.lang.page.Page}
 * construction.
 */
public class PageTest {

    @Test
    public void testDefaultPageLimit() {
        Page page = Page.first();
        Assert.assertEquals(0, page.skip());
        Assert.assertEquals(Page.DEFAULT_SIZE, page.limit());
    }

    @Test
    public void testPageLimit() {
        Page page = Page.at(3);
        Assert.assertEquals(2 * Page.DEFAULT_SIZE, page.skip());
    }

    @Test
    public void testPageLimitAndSkip() {
        Page page = Page.with(50).to(10);
        Assert.assertEquals(450, page.skip());
    }

    @Test
    public void testPageNext() {
        Page page = Page.at(45).next();
        Assert.assertEquals(45 * Page.DEFAULT_SIZE, page.skip());
        page = page.size(100).back();
        Assert.assertEquals(44 * 100, page.skip());
    }

}
