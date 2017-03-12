/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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
package com.cinchapi.concourse.server.storage.db;

import com.cinchapi.concourse.server.model.PrimaryKey;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.server.storage.db.Block;
import com.cinchapi.concourse.server.storage.db.SecondaryBlock;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.TestData;

/**
 * 
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

}
