/*
 * Copyright (c) 2013-2019 Cinchapi Inc.
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
package com.cinchapi.concourse.server.storage.db;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Tag;
import com.cinchapi.concourse.server.model.PrimaryKey;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.server.storage.Action;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.TestData;

/**
 * Unit tests for {@link SecondaryBlock}.
 * 
 * @author Jeff Nelson
 */
public class SecondaryBlockTest extends BlockTest<Text, Value, PrimaryKey> {

    @Override
    protected Text getLocator() {
        return TestData.getText();
    }

    @Override
    protected Value getKey() {
        return TestData.getValue();
    }

    @Override
    protected PrimaryKey getValue() {
        return TestData.getPrimaryKey();
    }

    @Override
    protected SecondaryBlock getMutableBlock(String directory) {
        return Block.createSecondaryBlock(Long.toString(Time.now()), directory);
    }

    @Test
    public void testGenerateBlockIndexWithEqualValuesOfDifferentTypes() {
        SecondaryBlock block = getMutableBlock(TestData.getTemporaryTestDir());
        block.insert(Text.wrapCached("payRangeMax"),
                Value.wrap(Convert.javaToThrift(18)), PrimaryKey.wrap(1),
                Time.now(), Action.ADD);
        block.insert(Text.wrapCached("payRangeMax"),
                Value.wrap(Convert.javaToThrift(new Double(18.0))),
                PrimaryKey.wrap(1), Time.now(), Action.ADD);
        block.insert(Text.wrapCached("payRangeMax"),
                Value.wrap(Convert.javaToThrift(new Double(625))),
                PrimaryKey.wrap(1), Time.now(), Action.ADD);

        block.insert(Text.wrapCached("payRangeMax"),
                Value.wrap(Convert.javaToThrift("foo")), PrimaryKey.wrap(1),
                Time.now(), Action.ADD);
        block.insert(Text.wrapCached("payRangeMax"),
                Value.wrap(Convert.javaToThrift(Tag.create("foo"))),
                PrimaryKey.wrap(1), Time.now(), Action.ADD);
        block.insert(Text.wrapCached("payRangeMax"),
                Value.wrap(Convert.javaToThrift(new Double(626))),
                PrimaryKey.wrap(1), Time.now(), Action.ADD);
        block.sync();
        Assert.assertTrue(true); // lack of Exception means the test passes
    }

}
