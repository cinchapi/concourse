/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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
package com.cinchapi.concourse.validate;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.common.base.ArrayBuilder;
import com.cinchapi.concourse.util.Random;
import com.cinchapi.concourse.validate.Keys.Key;
import com.cinchapi.concourse.validate.Keys.KeyType;

/**
 * Unit tests for {@link Keys}.
 *
 * @author Jeff Nelson
 */
public class KeysTest {

    @Test
    public void testParseWritableKey() {
        String input = Random.getSimpleString();
        Key key = Keys.parse(input);
        Assert.assertEquals(KeyType.WRITABLE_KEY, key.type());
    }

    @Test
    public void testParseNavigationKey() {
        ArrayBuilder<String> ab = ArrayBuilder.builder();
        for (int i = 0; i < Random.getScaleCount(); ++i) {
            ab.add(Random.getSimpleString());
        }
        String input = AnyStrings.join('.', (Object[]) ab.build());
        Key key = Keys.parse(input);
        Assert.assertEquals(KeyType.NAVIGATION_KEY, key.type());
        Assert.assertArrayEquals(ab.build(), key.data());
    }

    @Test
    public void testParseFunctionKey() {
        String input = "age | avg";
        Key key = Keys.parse(input);
        Assert.assertEquals(KeyType.FUNCTION_KEY, key.type());
    }

    @Test
    public void testParseIdentifierKey() {
        String input = "$id$";
        Key key = Keys.parse(input);
        Assert.assertEquals(KeyType.IDENTIFIER_KEY, key.type());
    }

    @Test
    public void testParseInvalidKey() {
        Assert.assertEquals(KeyType.INVALID_KEY, Keys.parse("").type());
        Assert.assertEquals(KeyType.INVALID_KEY, Keys.parse("$id").type());
        Assert.assertEquals(KeyType.INVALID_KEY,
                Keys.parse("invalid-key").type());
    }

}
