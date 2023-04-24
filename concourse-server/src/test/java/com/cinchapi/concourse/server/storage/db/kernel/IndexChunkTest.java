/*
 * Copyright (c) 2013-2023 Cinchapi Inc.
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
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.Tag;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.model.Identifier;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.server.storage.Action;
import com.cinchapi.concourse.server.storage.cache.BloomFilter;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.TestData;

/**
 * Unit test for
 * {@link com.cinchapi.concourse.server.storage.db.kernel.IndexChunk}.
 *
 * @author Jeff Nelson
 */
public class IndexChunkTest extends ChunkTest<Text, Value, Identifier> {

    @Override
    protected Text getLocator() {
        return TestData.getText();
    }

    @Override
    protected Value getKey() {
        return TestData.getValue();
    }

    @Override
    protected Identifier getValue() {
        return TestData.getIdentifier();
    }

    @Override
    protected Chunk<Text, Value, Identifier> create(BloomFilter filter) {
        return IndexChunk.create(filter);
    }

    @Override
    protected Chunk<Text, Value, Identifier> load(Path file, BloomFilter filter,
            Manifest manifest) {
        return IndexChunk.load(file, 0, FileSystem.getFileSize(file.toString()),
                filter, manifest);
    }

    @Test
    public void testGenerateManifestWithEqualValuesOfDifferentTypes() {
        chunk.insert(Text.wrapCached("payRangeMax"),
                Value.wrap(Convert.javaToThrift(18)), Identifier.of(1),
                Time.now(), Action.ADD);
        chunk.insert(Text.wrapCached("payRangeMax"),
                Value.wrap(Convert.javaToThrift(new Double(18.0))),
                Identifier.of(1), Time.now(), Action.ADD);
        chunk.insert(Text.wrapCached("payRangeMax"),
                Value.wrap(Convert.javaToThrift(new Double(625))),
                Identifier.of(1), Time.now(), Action.ADD);

        chunk.insert(Text.wrapCached("payRangeMax"),
                Value.wrap(Convert.javaToThrift("foo")), Identifier.of(1),
                Time.now(), Action.ADD);
        chunk.insert(Text.wrapCached("payRangeMax"),
                Value.wrap(Convert.javaToThrift(Tag.create("foo"))),
                Identifier.of(1), Time.now(), Action.ADD);
        chunk.insert(Text.wrapCached("payRangeMax"),
                Value.wrap(Convert.javaToThrift(new Double(626))),
                Identifier.of(1), Time.now(), Action.ADD);
        Map<?, ?> entries = Reflection.call(chunk.manifest(), "entries"); // authorized
        // Ensure 18.0 and 18 as well as `foo` and foo are treated as equal when
        // generating the index. That means there should be 5 entires (e.g. an
        // entry for the payRangeMax locator and 4 entries for that locator and
        // the 4 unique keys (18.0, foo, 625, 626).
        Assert.assertEquals(5, entries.size());
    }

}