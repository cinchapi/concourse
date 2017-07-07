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

import java.lang.reflect.Constructor;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import com.cinchapi.concourse.server.io.Byteables;
import com.cinchapi.concourse.server.storage.Action;
import com.cinchapi.concourse.server.storage.db.Revision;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.TestData;
import com.google.common.base.Throwables;

/**
 * Unit tests for all the subclasses of {@link Revision}.
 * 
 * @author Jeff Nelson
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
