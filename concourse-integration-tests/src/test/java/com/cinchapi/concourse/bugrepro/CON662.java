/*
 * Copyright (c) 2013-2020 Cinchapi Inc.
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
package com.cinchapi.concourse.bugrepro;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;

/**
 * Unit tests to repro the conditions in CON-661.
 *
 * @author Jeff Nelson
 */
public class CON662 extends ConcourseIntegrationTest {

    @Test
    public void test() {
        Criteria criteria = Criteria.parse("foo.bar eq 1");
        client.find(criteria);
        Assert.assertTrue(true); // lack of Exception means the test passes

    }

}
