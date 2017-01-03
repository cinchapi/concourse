/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse.util;

import java.net.URL;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link Resources} utility methods.
 * 
 * @author raghavbabu
 */
public class ResourcesTest {

    @Test
    public void testGet() {
        URL url = Resources.get("/college.csv");
        Assert.assertEquals(Resources.get("/college.csv"), url);
    }

}
