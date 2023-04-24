/*
 * Copyright (c) 2013-2023 Cinchapi Inc.
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
package com.cinchapi.concourse.server.io;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.base.ArrayBuilder;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.util.TestData;

/**
 * Unit tests for {@link Composite}.
 *
 * @author Jeff Nelson
 */
public class CompositeTest {

    /**
     * Return a {@link Byteable} array that will generate a large
     * {@link Composite}.
     * 
     * @return the {@link Byteable} array
     */
    private static Byteable[] getLargeParts() {
        int size = 0;
        ArrayBuilder<Byteable> ab = ArrayBuilder.builder();
        while (size < Composite.MAX_SIZE) {
            Byteable byteable = TestData.getText();
            size += byteable.getCanonicalLength();
            ab.add(byteable);
        }
        return ab.build();
    }

    @Test
    public void testCompositeMaxSize() {
        Composite composite = Composite.create(getLargeParts());
        Assert.assertEquals(Composite.MAX_SIZE, composite.size());
        System.out.println(composite);
    }

    @Test
    public void testCompositeMaxSizeConsistency() {
        Byteable[] parts = getLargeParts();
        Composite c1 = Composite.create(parts);
        Composite c2 = Composite.create(parts);
        Assert.assertEquals(c1, c2);
    }

    @Test
    public void testCompositeMaxSizeRoundTripConsistency() {
        Byteable[] parts = getLargeParts();
        Composite c1 = Composite.create(parts);
        Composite c2 = Composite.load(c1.getBytes());
        Assert.assertEquals(c1, c2);
    }

    @Test
    public void testReproCON_674() {
        Assert.assertNotEquals(Composite.create(Text.wrap("iqu")),
                Composite.create(Text.wrap("iq"), Text.wrap("u")));
    }

    @Test
    public void testReproCON_674Corner() {
        Byteable b = new Byteable() {

            @Override
            public void copyTo(ByteSink sink) {
                sink.putInt(0);
                sink.putUtf8("a");
                sink.putInt(1);
                sink.putUtf8("b");
            }

            @Override
            public int size() {
                return 10;
            }

        };
        Composite c1 = Composite.create(b);
        Composite c2 = Composite.create(Text.wrap("a"), Text.wrap("b"));
        Assert.assertNotEquals(c1, c2);
    }

    @Test
    public void testCompositeMaxSizeGetParts() {
        Byteable[] parts = getLargeParts();
        Composite composite = Composite.create(parts);
        Assert.assertArrayEquals(parts, composite.parts());
        composite = Composite.load(composite.getBytes());
        Assert.assertFalse(composite.hasParts());
    }

    @Test
    public void testCompositeMaxSizeCached() {
        Byteable[] parts = getLargeParts();
        Composite c1 = Composite.createCached(parts);
        Composite c2 = Composite.createCached(parts);
        while (c1 != c2) {
            c1 = Composite.createCached(parts);
            c2 = Composite.createCached(parts);
        }
        Composite c3 = Composite.createCached(parts);
        Composite c4 = Composite.create(parts);
        Assert.assertEquals(c1, c2);
        Assert.assertEquals(c1, c3);
        Assert.assertEquals(c1, c4);
    }
}
