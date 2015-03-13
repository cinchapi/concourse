/*
 * Copyright (c) 2013-2015 Cinchapi, Inc.
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
package org.cinchapi.concourse.util;

import org.cinchapi.concourse.ConcourseBaseTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link LongBitSet}.
 * 
 * @author Jeff Nelson
 */
public class LongBitSetTest extends ConcourseBaseTest {

    private LongBitSet bitSet;

    @Override
    public void beforeEachTest() {
        bitSet = LongBitSet.create();
    }

    @Test
    public void testSetTrue() {
        int position = getPosition();
        bitSet.set(position, true);
        Assert.assertEquals(true, bitSet.get(position));
    }

    @Test
    public void testDefaultIsFalse() {
        int position = getPosition();
        Assert.assertEquals(false, bitSet.get(position));
    }

    @Test
    public void testSetTrueThenFalse() {
        int position = getPosition();
        bitSet.set(position, true);
        bitSet.set(position, false);
        Assert.assertEquals(false, bitSet.get(position));
    }


    /**
     * Return a random position.
     * 
     * @return the position
     */
    private static int getPosition() {
        return TestData.getInt();
    }

}
