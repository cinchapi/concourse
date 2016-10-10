/*
 * Copyright (c) 2013-2016 Cinchapi Inc.
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
package com.cinchapi.concourse.util;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.util.Numbers;
import com.cinchapi.concourse.util.Random;

/**
 * Unit tests for {@link Numbers}.
 * 
 * @author Jeff Nelson
 */
public class NumbersTest {

    @Test
    public void testMax() {
        Assert.assertEquals(10, Numbers.max(1, 8, 2.4, 3.789, 10, -11));
    }

    @Test
    public void testMin() {
        Assert.assertEquals(-11, Numbers.min(1, 8, 2.4, 3.789, 10, -11));
    }

    @Test
    public void testCompareNumbers() {
        Number a = Random.getNegativeNumber();
        Number b = Random.getPositiveNumber();
        Assert.assertTrue(Numbers.compare(a, b) < 0);
    }

}
