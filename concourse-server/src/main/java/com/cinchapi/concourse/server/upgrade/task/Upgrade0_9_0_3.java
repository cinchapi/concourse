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
package com.cinchapi.concourse.server.upgrade.task;

import java.io.File;

import com.cinchapi.concourse.security.Role;
import com.cinchapi.concourse.security.UserService;
import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.upgrade.SmartUpgradeTask;

/**
 * An {@link UpgradeTask} that assigns the ADMIN user role to existing users.
 *
 * @author Jeff Nelson
 */
public class Upgrade0_9_0_3 extends SmartUpgradeTask {

    private static String ACCESS_FILE_BACKUP = GlobalState.ACCESS_FILE + ".bak";

    @Override
    public String getDescription() {
        return "Assign ADMIN user role to existing users";
    }

    @Override
    protected void doTask() {
        String accessFile = getServerInstallDirectory() + File.separator
                + GlobalState.ACCESS_FILE;
        String accessBackupFile = getServerInstallDirectory() + File.separator
                + ACCESS_FILE_BACKUP;
        if(FileSystem.hasFile(accessFile)) {
            FileSystem.copyBytes(accessFile, accessBackupFile);
            UserService users = UserService.create(accessFile);
            users.forEachUser(username -> users.setRole(username, Role.ADMIN));
            FileSystem.deleteFile(accessBackupFile);
        }

    }

}
