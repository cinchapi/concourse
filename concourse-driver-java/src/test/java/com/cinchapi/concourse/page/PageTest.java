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

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link com.cinchapi.concourse.page.Page} building
 * functionality.
 */
public class PageTest {

    @Test(expected = IllegalStateException.class)
    public void testCannotAddSymbolToBuiltCriteria() {
        Page page = Page.number(1).build();
        page.setNumber(2);
    }

    @Test
    public void testPageNumber() {
        Page page = Page.number(1).build();
        Assert.assertEquals(1, page.number());
        Assert.assertEquals(0, page.container());
    }

    @Test
    public void testPageContainer() {
        Page page = Page.number(1).container(0).build();
        Assert.assertEquals(1, page.number());
        Assert.assertEquals(0, page.container());
    }

    @Test
    public void testPageNext() {
        Page page = Page.number(1).container(0).next().build();
        Assert.assertEquals(2, page.number());
        Assert.assertEquals(0, page.container());
    }
}
