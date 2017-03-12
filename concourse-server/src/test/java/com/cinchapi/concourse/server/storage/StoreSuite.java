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
package com.cinchapi.concourse.server.storage;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.cinchapi.concourse.server.storage.db.DatabaseTest;
import com.cinchapi.concourse.server.storage.temp.BufferTest;
import com.cinchapi.concourse.server.storage.temp.QueueTest;

/**
 * 
 * 
 * @author Jeff Nelson
 */
@RunWith(Suite.class)
@SuiteClasses({ BufferTest.class, QueueTest.class, EngineTest.class,
        EngineAtomicOperationTest.class, DatabaseTest.class,
        TransactionTest.class, TransactionAtomicOperationTest.class,
        StoresTest.class, TransactionGarbageCollectionTest.class })
public class StoreSuite {

}
