/*
 * Copyright (c) 2013-2018 Cinchapi Inc.
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

import java.io.File;

import com.cinchapi.concourse.security.AccessManager;
import com.cinchapi.concourse.security.LegacyAccessManager;
import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.upgrade.SmartUpgradeTask;

/**
 * Upgrade pre-0.5.0 user credentials to work with new {@link AccessManager}.
 * 
 * @author knd
 */
public class Upgrade0_5_0_1 extends SmartUpgradeTask {

    private static String ACCESS_FILE_BACKUP = GlobalState.ACCESS_FILE + ".bak";

    @Override
    public String getDescription() {
        return "Upgrading pre-0.5.0 credentials to assign user id.";
    }

    @Override
    protected void doTask() {
        String accessFile = getServerInstallDirectory() + File.separator
                + GlobalState.ACCESS_FILE;
        String accessBackupFile = getServerInstallDirectory() + File.separator
                + ACCESS_FILE_BACKUP;
        if(FileSystem.hasFile(accessFile)) {
            FileSystem.copyBytes(accessFile, accessBackupFile);
            FileSystem.deleteFile(accessFile);
            LegacyAccessManager legacyManager = LegacyAccessManager
                    .create(accessBackupFile);
            AccessManager manager = AccessManager.create(accessFile);
            legacyManager.transferCredentials(manager);
            FileSystem.deleteFile(accessBackupFile);
        }
    }

}
