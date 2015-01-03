/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2015 Jeff Nelson, Cinchapi Software Collective
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.cinchapi.concourse.server.storage.db;

import org.cinchapi.concourse.server.model.PrimaryKey;
import org.cinchapi.concourse.server.model.Text;
import org.cinchapi.concourse.server.model.Value;
import org.cinchapi.concourse.server.storage.Action;
import org.cinchapi.concourse.server.storage.db.Block;
import org.cinchapi.concourse.server.storage.db.PrimaryBlock;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.Convert;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Test;

/**
 * 
 * 
 * @author jnelson
 */
public class PrimaryBlockTest extends BlockTest<PrimaryKey, Text, Value> {
    
    @Test
    public void testInsertLocatorWithTrailingWhiteSpace() {
        PrimaryKey locator = PrimaryKey.wrap(1);
        Text key = Text.wrap("Youtube Embed Link ");
        Value value = Value.wrap(Convert.javaToThrift("http://youtube.com"));
        block.insert(locator, key, value, Time.now(), Action.ADD);
        Record<PrimaryKey, Text, Value> record = Record.createPrimaryRecordPartial(locator, key);
        block.sync();
        block.seek(locator, key, record);
        Assert.assertTrue(record.get(key).contains(value));
    }

    @Override
    protected PrimaryKey getLocator() {
        return TestData.getPrimaryKey();
    }

    @Override
    protected Text getKey() {
        return TestData.getText();
    }

    @Override
    protected Value getValue() {
        return TestData.getValue();
    }

    @Override
    protected PrimaryBlock getMutableBlock(String directory) {
        return Block.createPrimaryBlock(Long.toString(Time.now()), directory);
    }

}
