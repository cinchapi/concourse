/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013 Jeff Nelson, Cinchapi Software Collective
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
package org.cinchapi.concourse.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import org.cinchapi.concourse.server.model.Position;
import org.cinchapi.concourse.server.model.Text;
import org.cinchapi.concourse.server.model.Value;
import org.cinchapi.concourse.server.model.PrimaryKey;
import org.cinchapi.concourse.server.storage.Action;
import org.cinchapi.concourse.server.storage.db.PrimaryRevision;
import org.cinchapi.concourse.server.storage.db.Revision;
import org.cinchapi.concourse.server.storage.db.SearchRevision;
import org.cinchapi.concourse.server.storage.db.SecondaryRevision;
import org.cinchapi.concourse.server.storage.temp.Write;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.time.Time;

import com.beust.jcommander.internal.Lists;
import com.google.common.base.Throwables;

/**
 * A utility class for getting test data.
 * 
 * @author jnelson
 */
public final class TestData extends Random {

    public static final String DATA_DIR = "test.out/buffer";

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
