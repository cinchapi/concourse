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
package com.cinchapi.concourse.server.ops;

import java.io.File;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Link;
import com.cinchapi.concourse.server.storage.AtomicSupport;
import com.cinchapi.concourse.server.storage.Engine;
import com.cinchapi.concourse.server.storage.temp.Write;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.TestData;

/**
 * Unit tests for {@link Operations}
 *
 * @author Jeff Nelson
 */
public class OperationsTest {

    protected void setupGraph(AtomicSupport store) {
        store.accept(Write.add("foo", Convert.javaToThrift(Link.to(2)), 1));
        store.accept(Write.add("name", Convert.javaToThrift("A"), 1));
        store.accept(Write.add("baz", Convert.javaToThrift(Link.to(3)), 1));
        store.accept(Write.add("baz", Convert.javaToThrift(Link.to(4)), 1));
        store.accept(Write.add("foo", Convert.javaToThrift(Link.to(3)), 2));
        store.accept(Write.add("bar", Convert.javaToThrift(Link.to(1)), 2));
        store.accept(Write.add("bar", Convert.javaToThrift(Link.to(4)), 2));
        store.accept(Write.add("name", Convert.javaToThrift("B"), 2));
        store.accept(Write.add("bar", Convert.javaToThrift(Link.to(2)), 3));
        store.accept(Write.add("name", Convert.javaToThrift("C"), 3));
        store.accept(Write.add("baz", Convert.javaToThrift(Link.to(1)), 3));
        store.accept(Write.add("baz", Convert.javaToThrift(Link.to(2)), 3));
        store.accept(Write.add("baz", Convert.javaToThrift(Link.to(5)), 3));
        store.accept(Write.add("foo", Convert.javaToThrift(Link.to(1)), 4));
        store.accept(Write.add("foo", Convert.javaToThrift(Link.to(2)), 4));
        store.accept(Write.add("bar", Convert.javaToThrift(Link.to(3)), 4));
        store.accept(Write.add("name", Convert.javaToThrift("D"), 4));
        store.accept(Write.add("baz", Convert.javaToThrift(Link.to(5)), 4));
        store.accept(Write.add("baz", Convert.javaToThrift(Link.to(3)), 4));
        store.accept(Write.add("name", Convert.javaToThrift("E"), 5));
    }

    @Test
    public void testNavigateKeyRecordAtomic() {
        AtomicSupport store = getStore();
        try {
            setupGraph(store);
            String key = "bar.baz.name";
            Map<Long, Set<TObject>> data = Operations.navigateKeyRecordAtomic(
                    key, 3, Time.NONE, store.startAtomicOperation());
            Assert.assertTrue(data.isEmpty());
        }
        finally {
            store.stop();
        }
    }

    @Test
    public void testBrowseNavigationKeyAtomic() {
        AtomicSupport store = getStore();
        try {
            setupGraph(store);
            String key = "foo.bar.baz.name";
            Map<TObject, Set<Long>> data = Operations.browseNavigationKeyAtomic(
                    key, Time.NONE, store.startAtomicOperation());
            System.out.println(data);
            //TODO: add Assert
        }
        finally {
            store.stop();
        }
    }

    /**
     * Return an {@link AtomicSupport} {@link Store} that can be used in unit
     * tests.
     * 
     * @return an {@link AtomicSupport} store
     */
    protected AtomicSupport getStore() {
        String directory = TestData.DATA_DIR + File.separator + Time.now();
        Engine store = new Engine(directory + File.separator + "buffer",
                directory + File.separator + "database");
        store.start();
        return store;
    }

}
