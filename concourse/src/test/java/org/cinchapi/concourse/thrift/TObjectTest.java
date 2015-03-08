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
package org.cinchapi.concourse.thrift;

import org.cinchapi.concourse.Tag;
import org.cinchapi.concourse.util.Convert;
import org.cinchapi.concourse.util.Random;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the constraints promised by the {@link TObject} class.
 * 
 * @author Jeff Nelson
 */
public class TObjectTest {

    @Test
    public void testTagAndStringAreEqual() {
        String base = Random.getString();
        TObject string = Convert.javaToThrift(base);
        TObject tag = Convert.javaToThrift(Tag.create(base));
        Assert.assertEquals(tag, string);
    }

    @Test
    public void testTagAndStringHaveSameHashCode() {
        String base = Random.getString();
        TObject string = Convert.javaToThrift(base);
        TObject tag = Convert.javaToThrift(Tag.create(base));
        Assert.assertEquals(tag.hashCode(), string.hashCode());
    }

    @Test
    public void testTagAndStringDoNotMatch() {
        String base = Random.getString();
        TObject string = Convert.javaToThrift(base);
        TObject tag = Convert.javaToThrift(Tag.create(base));
        Assert.assertFalse(tag.matches(string));
        Assert.assertFalse(string.matches(tag));
    }

}
