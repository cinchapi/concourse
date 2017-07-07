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
package com.cinchapi.concourse.server.model;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.server.io.ByteableTest;
import com.cinchapi.concourse.server.model.PrimaryKey;
import com.cinchapi.concourse.util.TestData;

/**
 * Unit tests for {@link PrimaryKey}.
 * 
 * @author Jeff Nelson
 */
public class PrimaryKeyTest extends ByteableTest {

    @Test
    public void testCompareTo() {
        long value1 = TestData.getLong();
        value1 = value1 == Long.MAX_VALUE ? value1 - 1 : value1;
        long value2 = value1 + 1;
        PrimaryKey key1 = PrimaryKey.wrap(value1);
        PrimaryKey key2 = PrimaryKey.wrap(value2);
        Assert.assertTrue(key1.compareTo(key2) < 0);
    }

    @Test
    public void testSize() {
        PrimaryKey key = TestData.getPrimaryKey();
        Assert.assertEquals(PrimaryKey.SIZE, key.size());
        Assert.assertEquals(PrimaryKey.SIZE, key.getBytes().capacity());
    }

    @Override
    protected Class<PrimaryKey> getTestClass() {
        return PrimaryKey.class;
    }

}
