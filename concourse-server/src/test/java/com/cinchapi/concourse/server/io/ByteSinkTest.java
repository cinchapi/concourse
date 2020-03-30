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

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link ByteSink}.
 *
 * @author Jeff Nelson
 */
public class ByteSinkTest {

    @Test
    public void testNullByteSinkTracksPosition() {
        ByteSink a = ByteSink.to(ByteBuffer.allocate(100));
        ByteSink b = ByteSink.toDevNull();
        a.put((byte) 1);
        b.put((byte) 1);
        Assert.assertEquals(a.position(), b.position());
        a.putShort((short) 1);
        b.putShort((short) 1);
        Assert.assertEquals(a.position(), b.position());
        a.putInt(1);
        b.putInt(1);
        Assert.assertEquals(a.position(), b.position());
        a.putLong(1);
        b.putLong(1);
        Assert.assertEquals(a.position(), b.position());
        a.putFloat(1);
        b.putFloat(1);
        Assert.assertEquals(a.position(), b.position());
        a.putDouble(1);
        b.putDouble(1);
        Assert.assertEquals(a.position(), b.position());
        a.putUtf8("hello");
        b.putUtf8("hello");
        Assert.assertEquals(a.position(), b.position());
        a.put(ByteBuffer.allocate(10));
        b.put(ByteBuffer.allocate(10));
        Assert.assertEquals(a.position(), b.position());
        a.put(new byte[5]);
        b.put(new byte[5]);
        Assert.assertEquals(a.position(), b.position());
    }

}
