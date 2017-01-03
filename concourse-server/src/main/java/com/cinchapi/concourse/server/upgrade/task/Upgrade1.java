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
package com.cinchapi.concourse.server.upgrade.task;

import com.cinchapi.concourse.server.upgrade.UpgradeTask;

/**
 * "Bootstrap" the framework by adding the schema metadata in the appropriate
 * directories.
 * 
 * @author Jeff Nelson
 */
public class Upgrade1 extends UpgradeTask {

    @Override
    public String getDescription() {
        return "Bootstraping the upgrade framework";
    }

    @Override
    public int version() {
        return 1;
    }

    @Override
    protected void doTask() {
        logInfoMessage("The upgrade framework has been bootstrapped");

    }

}
