/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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
package com.cinchapi.concourse.server.upgrade.task;

import java.util.Iterator;

import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.model.PrimaryKey;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.server.storage.Inventory;
import com.cinchapi.concourse.server.storage.db.Database;
import com.cinchapi.concourse.server.storage.db.Revision;
import com.cinchapi.concourse.server.storage.temp.Buffer;
import com.cinchapi.concourse.server.storage.temp.Write;
import com.cinchapi.concourse.server.upgrade.SmartUpgradeTask;
import com.cinchapi.concourse.util.Environments;

/**
 * TODO add integration tests!!
 * 
 * @author Jeff Nelson
 */
public class Upgrade0_5_0_2 extends SmartUpgradeTask {

    @Override
    public String getDescription() {
        return "Populate the inventory with existing records";
    }

    @Override
    protected void doTask() {
        Iterator<String> envIt = Environments.iterator(
                GlobalState.BUFFER_DIRECTORY, GlobalState.DATABASE_DIRECTORY);
        while (envIt.hasNext()) {
            String env = envIt.next();
            Inventory inventory = Inventory.create(FileSystem.makePath(
                    GlobalState.BUFFER_DIRECTORY, env, "meta", "inventory"));
            // Get records from thTexttabase via the Primary Blocks
            String dbStore = FileSystem.makePath(
                    GlobalState.DATABASE_DIRECTORY, env);
            Iterator<Revision<PrimaryKey, Text, Value>> dbIt = Database
                    .onDiskStreamingIterator(dbStore);
            while (dbIt.hasNext()) {
                Revision<PrimaryKey, Text, Value> revision = dbIt.next();
                inventory.add(revision.getLocator().longValue());
            }
            // Get records from the Buffer
            String bufferStore = FileSystem.makePath(
                    GlobalState.BUFFER_DIRECTORY, env);
            Iterator<Write> bufIt = Buffer.onDiskIterator(bufferStore);
            while (bufIt.hasNext()) {
                Write write = bufIt.next();
                inventory.add(write.getRecord().longValue());
            }
            inventory.sync();
        }
    }

}
