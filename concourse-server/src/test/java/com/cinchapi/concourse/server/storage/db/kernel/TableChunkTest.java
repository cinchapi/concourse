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
package com.cinchapi.concourse.server.storage.db.kernel;

import java.nio.file.Path;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.server.io.Composite;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.model.Identifier;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.server.storage.Action;
import com.cinchapi.concourse.server.storage.cache.BloomFilter;
import com.cinchapi.concourse.server.storage.db.TableRecord;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.TestData;

/**
 * Unit test for
 * {@link com.cinchapi.concourse.server.storage.db.kernel.TableChunk}.
 *
 * @author Jeff Nelson
 */
public class TableChunkTest extends ChunkTest<Identifier, Text, Value> {

    @Override
    protected Identifier getLocator() {
        return TestData.getIdentifier();
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
    protected Chunk<Identifier, Text, Value> create(BloomFilter filter) {
        return TableChunk.create(filter);
    }

    @Override
    protected Chunk<Identifier, Text, Value> load(Path file, BloomFilter filter,
            Manifest manifest) {
        return TableChunk.load(file, 0, FileSystem.getFileSize(file.toString()),
                filter, manifest);
    }

    @Test
    public void testInsertLocatorWithTrailingWhiteSpace() {
        Identifier locator = Identifier.of(1);
        Text key = Text.wrap("Youtube Embed Link ");
        Value value = Value.wrap(Convert.javaToThrift("http://youtube.com"));
        chunk.insert(locator, key, value, Time.now(), Action.ADD);
        TableRecord record = TableRecord.createPartial(locator, key);
        chunk.transfer(file);
        chunk = load(file, filter, chunk.manifest());
        chunk.seek(Composite.create(locator, key), record);
        Assert.assertTrue(record.get(key).contains(value));
    }

}
