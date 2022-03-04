/*
 * Copyright (c) 2013-2022 Cinchapi Inc.
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
package com.cinchapi.concourse.server.storage.format;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import org.apache.commons.compress.utils.Lists;

import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.storage.db.kernel.Segment;
import com.cinchapi.concourse.server.storage.db.kernel.SegmentLoadingException;
import com.cinchapi.concourse.util.Logger;

/**
 * Limited ability to handle Storage Format Version 3 data files
 *
 * @author Jeff Nelson
 */
public final class StorageFormatV3 {

    /**
     * Load the {@link Segment Segments} from {@code directory}
     * 
     * @param directory
     * @return an {@link Iterable} containing all the loaded {@link Segment
     *         Segments}
     */
    public static Iterable<Segment> load(Path directory) {
        /*
         * NOTE: This implementation is copied from Database#start with a few
         * changes:
         * 1) Segments are loaded seriously instead of asynchronously
         * 2) Overlapping Segments are not removed
         */
        List<Segment> segments = Lists.newArrayList();
        Stream<Path> files = FileSystem.ls(directory);
        files.forEach(file -> {
            try {
                Segment segment = Segment.load(file);
                segments.add(segment);
            }
            catch (SegmentLoadingException e) {
                Logger.error("Error when trying to load Segment {}", file);
                Logger.error("", e);
            }
        });
        files.close();

        // Sort the segments in chronological order
        Collections.sort(segments, Segment.TEMPORAL_COMPARATOR);
        return segments;
    }

}
