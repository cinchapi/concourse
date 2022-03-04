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
 * Tests for {@link com.cinchapi.concourse.server.model.Position}.
 *
 * @author Jeff Nelson
 */
public class PositionTest extends ByteableTest {

    @Test
    public void testCompareToSamePrimaryKeyAndSameIndex() {
        Identifier key = TestData.getIdentifier();
        int index = Math.abs(TestData.getInt());
        Position p1 = Position.of(key, index);
        Position p2 = Position.of(key, index);
        Assert.assertTrue(p1.compareTo(p2) == 0);
    }

    @Test
    public void testCompareToSamePrimaryKeyAndDiffIndex() {
        Identifier key = TestData.getIdentifier();
        int index1 = Math.abs(TestData.getInt());
        index1 = index1 == Integer.MAX_VALUE ? index1 - 1 : index1;
        int index2 = index1 + 1;
        Position p1 = Position.of(key, index1);
        Position p2 = Position.of(key, index2);
        Assert.assertTrue(p1.compareTo(p2) < 0);
    }

    @Test
    public void testCompareToDiffPrimaryKey() {
        long long1 = TestData.getLong();
        long1 = long1 == Long.MAX_VALUE ? long1 - 1 : long1;
        long long2 = long1 + 1;
        Identifier key1 = Identifier.of(long1);
        Identifier key2 = Identifier.of(long2);
        Position p1 = Position.of(key1, Math.abs(TestData.getInt()));
        Position p2 = Position.of(key2, Math.abs(TestData.getInt()));
        Assert.assertTrue(p1.compareTo(p2) < 0);
    }

    @Test
    public void testSizeForByteSizeIndex() {
        Position p = Position.of(TestData.getIdentifier(),
                Math.abs(TestData.getInt()) % Byte.MAX_VALUE);
        Assert.assertEquals(Position.SIZE, p.size());
    }

    @Test
    public void testSizeForShortSizeIndex() {
        Position p = Position.of(TestData.getIdentifier(),
                (Math.abs(TestData.getInt()) % Short.MAX_VALUE)
                        + Byte.MAX_VALUE);
        Assert.assertEquals(Position.SIZE, p.size());
    }

    @Test
    public void testSizeForIntSizeIndex() {
        Position p = Position.of(TestData.getIdentifier(),
                (Math.abs(TestData.getInt()) % Integer.MAX_VALUE)
                        + Short.MAX_VALUE);
        Assert.assertEquals(Position.SIZE, p.size());
    }

    @Override
    protected Class<Position> getTestClass() {
        return Position.class;
    }

}
