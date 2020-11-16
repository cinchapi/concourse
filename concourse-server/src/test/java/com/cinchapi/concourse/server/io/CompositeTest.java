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
package com.cinchapi.concourse.server.io;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.server.model.Text;

/**
 * Unit tests for {@link Composite}.
 *
 * @author Jeff Nelson
 */
public class CompositeTest {

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
}
