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
package com.cinchapi.concourse;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.util.Numbers;

/**
 * Unit tests that cover corner cases of the calculate methods.
 *
 * @author Jeff Nelson
 */
public class CornerCaseCalculateTest extends ConcourseIntegrationTest {
    
    @Test
    public void testMaxKeyRecordIsNullWhenNoValuesPresent() {
        Number value = client.calculate().max("age", 1);
        Assert.assertNull(value);
    }
    
    @Test
    public void testMaxKeyIsNullWhenNoValuesPresent() {
        Number value = client.calculate().max("age");
        Assert.assertNull(value);
    }
    
    @Test
    public void testMinKeyRecordIsNullWhenNoValuesPresent() {
        Number value = client.calculate().min("age", 1);
        Assert.assertNull(value);
    }
    
    @Test
    public void testMinKeyIsNullWhenNoValuesPresent() {
        Number value = client.calculate().min("age");
        Assert.assertNull(value);
    }
    
    @Test
    public void testSumKeyRecordIsNullWhenNoValuesPresent() {
        Number value = client.calculate().sum("age", 1);
        Assert.assertNull(value);
    }
    
    @Test
    public void testSumKeyIsNullWhenNoValuesPresent() {
        Number value = client.calculate().sum("age");
        Assert.assertNull(value);
    }
    
    @Test
    public void testCountKeyRecordIsZeroWhenNoValuesPresent() {
        Number value = client.calculate().count("age", 1);
        Assert.assertTrue(Numbers.areEqual(0, value));
    }
    
    @Test
    public void testCountKeyIsZeroWhenNoValuesPresent() {
        Number value = client.calculate().count("age");
        Assert.assertTrue(Numbers.areEqual(0, value));
    }
    
    @Test
    public void testAveragetKeyRecordIsNullWhenNoValuesPresent() {
        Number value = client.calculate().average("age", 1);
        Assert.assertNull(value);
    }
    
    @Test
    public void testAveragetKeyIsNullWhenNoValuesPresent() {
        Number value = client.calculate().average("age");
        Assert.assertNull(value);
    }

}
