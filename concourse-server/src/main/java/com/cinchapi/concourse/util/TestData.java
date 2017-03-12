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
package com.cinchapi.concourse.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import com.beust.jcommander.internal.Lists;
import com.cinchapi.concourse.server.model.Position;
import com.cinchapi.concourse.server.model.PrimaryKey;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.server.storage.Action;
import com.cinchapi.concourse.server.storage.db.PrimaryRevision;
import com.cinchapi.concourse.server.storage.db.Revision;
import com.cinchapi.concourse.server.storage.db.SearchRevision;
import com.cinchapi.concourse.server.storage.db.SecondaryRevision;
import com.cinchapi.concourse.server.storage.temp.Write;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.Random;
import com.google.common.base.Throwables;

/**
 * A utility class for getting test data.
 * 
 * @author Jeff Nelson
 */
public final class TestData extends Random {

    public static final String DATA_DIR = "test.out";

    /**
     * Return a temporary file to use for testing.
     * 
     * @return the file path
     */
    public static String getTemporaryTestFile() {
        return DATA_DIR + File.separator + Time.now() + ".tmp";
    }

    /**
     * Return a temporary directory to use for testing files.
     * 
     * @return the directory path
     */
    public static String getTemporaryTestDir() {
        return DATA_DIR + File.separator + Time.now();
    }

    public static PrimaryRevision getPrimaryRevision() {
        return Revision.createPrimaryRevision(getPrimaryKey(), getText(),
                getValue(), Time.now(), Action.ADD);
    }

    public static SearchRevision getSearchRevision() {
        return Revision.createSearchRevision(getText(), getText(),
                getPosition(), Time.now(), Action.ADD);
    }

    public static SecondaryRevision getSecondaryRevision() {
        return Revision.createSecondaryRevision(getText(), getValue(),
                getPrimaryKey(), Time.now(), Action.ADD);
    }

    /**
     * Return a random {@link Position}.
     * 
     * @return a Position
     */
    public static Position getPosition() {
        return Position.wrap(getPrimaryKey(), Math.abs(getInt()));
    }

    public static PrimaryKey getPrimaryKey() {
        return PrimaryKey.wrap(getLong());
    }

    /**
     * Return a random {@link Text}.
     * 
     * @return a Text
     */
    public static Text getText() {
        return Text.wrap(getString());
    }

    /**
     * Return a random {@link TObject}
     * 
     * @return a TObject
     */
    public static TObject getTObject() {
        return Convert.javaToThrift(getObject());
    }

    public static Value getValue() {
        return Value.wrap(getTObject());
    }

    public static Write getWriteAdd() {
        return Write.add(getString(), getTObject(), getLong());
    }

    public static Write getWriteRemove() {
        return Write.remove(getString(), getTObject(), getLong());
    }

    public static Write getWriteNotStorable() {
        return Write.notStorable(getString(), getTObject(), getLong());
    }

    /**
     * Get each line from the words.txt file
     * 
     * @return
     */
    public static Iterable<String> getWordsDotTxt() {
        try {
            File file = new File(TestData.class.getResource("/words.txt")
                    .getFile());
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line;
            List<String> lines = Lists.newArrayList();
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
            reader.close();
            return lines;
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }

    }

    private TestData() {/* Utility class */}

}
