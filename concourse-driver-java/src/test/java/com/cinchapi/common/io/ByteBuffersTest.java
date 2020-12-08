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
package com.cinchapi.common.io;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.util.Random;
import com.google.common.base.Strings;

/**
 * Unit tests for {@link ByteBuffers} utility methods
 *
 * @author Jeff Nelson
 */
public class ByteBuffersTest {

    // NOTE: ByteBuffers lives in accent4j, but the unit tests are here because
    // they rely on internal Concourse libraries (e.g. RandomStringGenerator,
    // etc).

    @Test
    public void testEncodeToAndDecodeFromHexRoundTrip() {
        String expected = "admin";
        String actual = ByteBuffers.getUtf8String(ByteBuffers.decodeFromHexString(
                ByteBuffers.encodeAsHexString(ByteBuffers.fromUtf8String(expected))));
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testDeserializeStringTrailingWhitespace() {
        String string = Random.getString().trim();
        int numWhiteSpaces = Random.getScaleCount();
        string = Strings.padEnd(string, string.length() + numWhiteSpaces, ' ');
        ByteBuffer bytes = ByteBuffer
                .wrap(string.getBytes(StandardCharsets.UTF_8));
        String string2 = ByteBuffers.getString(bytes, StandardCharsets.UTF_8);
        Assert.assertEquals(string, string2);
    }

    @Test
    public void testDeserializeStringLeadingWhitespace() {
        String string = Random.getString().trim();
        int numWhiteSpaces = Random.getScaleCount();
        string = Strings.padStart(string, string.length() + numWhiteSpaces,
                ' ');
        ByteBuffer bytes = ByteBuffer
                .wrap(string.getBytes(StandardCharsets.UTF_8));
        String string2 = ByteBuffers.getString(bytes, StandardCharsets.UTF_8);
        Assert.assertEquals(string, string2);
    }

    @Test
    public void testDeserializeStringLeadingAndTrailingWhitespace() {
        String string = Random.getString().trim();
        int numWhiteSpaces1 = Random.getScaleCount();
        int numWhiteSpaces2 = Random.getScaleCount();
        string = Strings.padStart(string, string.length() + numWhiteSpaces1,
                ' ');
        string = Strings.padEnd(string, string.length() + numWhiteSpaces2, ' ');
        ByteBuffer bytes = ByteBuffer
                .wrap(string.getBytes(StandardCharsets.UTF_8));
        String string2 = ByteBuffers.getString(bytes, StandardCharsets.UTF_8);
        Assert.assertEquals(string, string2);
    }

    @Test
    public void testDeserializeStringNoWhitespace() {
        String string = Random.getString().trim();
        ByteBuffer bytes = ByteBuffer
                .wrap(string.getBytes(StandardCharsets.UTF_8));
        String string2 = ByteBuffers.getString(bytes, StandardCharsets.UTF_8);
        Assert.assertEquals(string, string2);
    }

}
