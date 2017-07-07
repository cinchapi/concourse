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
package com.cinchapi.concourse.bugrepro;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.test.ConcourseIntegrationTest;

/**
 * Unit test to verify the fix for GH-113.
 * 
 * @author Jeff Nelson
 */
public class GH113 extends ConcourseIntegrationTest {
    
    @Test
    public void repro(){
        long record = client.add("location", "Atlanta (HQ)");
        Assert.assertTrue(client.find("location = 'Atlanta (HQ)'").contains(record));
    }

}
