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
package com.cinchapi.concourse.server.storage.db.kernel;

import java.nio.file.Path;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.server.io.Composite;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.model.PrimaryKey;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.server.storage.Action;
import com.cinchapi.concourse.server.storage.cache.BloomFilter;
import com.cinchapi.concourse.server.storage.db.TableRecord;
import com.cinchapi.concourse.server.storage.db.kernel.Chunk.Folio;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.TestData;

/**
 * Unit test for
 * {@link com.cinchapi.concourse.server.storage.db.kernel.TableChunk}.
 *
 * @author Jeff Nelson
 */
public class TableChunkTest extends ChunkTest<PrimaryKey, Text, Value> {

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
    protected Chunk<PrimaryKey, Text, Value> create(BloomFilter filter) {
        return TableChunk.create(filter);
    }

    @Override
    protected Chunk<PrimaryKey, Text, Value> load(Path file, BloomFilter filter,
            Manifest manifest) {
        return TableChunk.load(file, 0, FileSystem.getFileSize(file.toString()),
                filter, manifest);
    }

    @Test
    public void testInsertLocatorWithTrailingWhiteSpace() {
        PrimaryKey locator = PrimaryKey.wrap(1);
        Text key = Text.wrap("Youtube Embed Link ");
        Value value = Value.wrap(Convert.javaToThrift("http://youtube.com"));
        chunk.insert(locator, key, value, Time.now(), Action.ADD);
        TableRecord record = TableRecord.createPartial(locator, key);
        Folio folio = chunk.serialize();
        FileSystem.writeBytes(folio.bytes(), file.toString());
        chunk = load(file, filter, folio.manifest());
        chunk.seek(Composite.create(locator, key), record);
        Assert.assertTrue(record.fetch(key).contains(value));
    }

}
