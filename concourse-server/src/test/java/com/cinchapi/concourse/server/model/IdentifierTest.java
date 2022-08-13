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
package com.cinchapi.concourse.server.model;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.server.io.ByteableTest;
import com.cinchapi.concourse.util.TestData;

/**
 * Unit tests for {@link com.cinchapi.concourse.server.model.Identifier}.
 *
 * @author Jeff Nelson
 */
public class IdentifierTest extends ByteableTest {

    @Test
    public void testCompareTo() {
        long value1 = TestData.getLong();
        value1 = value1 == Long.MAX_VALUE ? value1 - 1 : value1;
        long value2 = value1 + 1;
        Identifier key1 = Identifier.of(value1);
        Identifier key2 = Identifier.of(value2);
        Assert.assertTrue(key1.compareTo(key2) < 0);
    }

    @Test
    public void testSize() {
        Identifier key = TestData.getIdentifier();
        Assert.assertEquals(Identifier.SIZE, key.size());
        Assert.assertEquals(Identifier.SIZE, key.getBytes().capacity());
    }

    @Override
    protected Class<Identifier> getTestClass() {
        return Identifier.class;
    }

}
