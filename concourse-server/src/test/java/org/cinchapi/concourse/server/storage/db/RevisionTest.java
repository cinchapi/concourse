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
package org.cinchapi.concourse.server.storage.db;

import java.lang.reflect.Constructor;

import org.cinchapi.concourse.server.io.Byteables;
import org.cinchapi.concourse.server.storage.Action;
import org.cinchapi.concourse.server.storage.db.Revision;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import com.google.common.base.Throwables;

/**
 * Unit tests for all the subclasses of {@link Revision}.
 * 
 * @author jnelson
 */
@RunWith(Theories.class)
public class RevisionTest {

    public static @DataPoints
    Revision<?, ?, ?>[] revisions = { TestData.getPrimaryRevision(),
            TestData.getSearchRevision(), TestData.getSecondaryRevision() };

    @Test
    @Theory
    public void testSerialization(Revision<?, ?, ?> revision) {
        Assert.assertTrue(Byteables.read(revision.getBytes(),
                revision.getClass()).equals(revision));
    }

    @Test
    @Theory
    public void testEquals(Revision<?, ?, ?> revision) {
        Assert.assertEquals(revision, duplicate(revision));
    }

    @Test
    @Theory
    public void testHashCode(Revision<?, ?, ?> revision) {
        Assert.assertEquals(revision.hashCode(), duplicate(revision).hashCode());
    }

    /**
     * Duplicate {@code revision} with a different version.
     * 
     * @param revision
     * @return the duplicated revision
     */
    private Revision<?, ?, ?> duplicate(Revision<?, ?, ?> revision) {
        Constructor<?> constructor;
        try {
            constructor = revision.getClass().getDeclaredConstructor(
                    revision.getLocator().getClass(),
                    revision.getKey().getClass(),
                    revision.getValue().getClass(), Long.TYPE, Action.class);
            constructor.setAccessible(true);
            return (Revision<?, ?, ?>) constructor.newInstance(
                    revision.getLocator(), revision.getKey(),
                    revision.getValue(), Time.now(), revision.getType());
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }

    }

}
