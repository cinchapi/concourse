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
package com.cinchapi.concourse.server.storage;

import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.cinchapi.concourse.thrift.TObject;

/**
 * Base class for tests against a {@link Store}.
 *
 * @author Jeff Nelson
 */
public abstract class AbstractStoreTest extends ConcourseBaseTest {
    
    protected Store store;

    private int pref = GlobalState.MAX_SEARCH_SUBSTRING_LENGTH;

    @Rule
    public TestRule watcher = new TestWatcher() {

        @Override
        protected void finished(Description desc) {
            store.stop();
            cleanup(store);
            GlobalState.MAX_SEARCH_SUBSTRING_LENGTH = pref;
        }

        @Override
        protected void starting(Description desc) {
            store = getStore();
            store.start();
            // Don't allow dev preferences to interfere with unit test logic...
            GlobalState.MAX_SEARCH_SUBSTRING_LENGTH = -1;

        }
    };

    /**
     * Add {@code key} as {@code value} to {@code record} in the {@code store}.
     * 
     * @param key
     * @param value
     * @param record
     */
    protected abstract void add(String key, TObject value, long record);

    /**
     * Cleanup the store and release and resources, etc.
     * 
     * @param store
     */
    protected abstract void cleanup(Store store);

    /**
     * Return a Store for testing.
     * 
     * @return the Store
     */
    protected abstract Store getStore();

    /**
     * Remove {@code key} as {@code value} from {@code record} in {@code store}.
     * 
     * @param key
     * @param value
     * @param record
     */
    protected abstract void remove(String key, TObject value, long record);

}
