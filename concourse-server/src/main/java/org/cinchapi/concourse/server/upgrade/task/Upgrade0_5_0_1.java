/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2015 Jeff Nelson, Cinchapi Software Collective
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
package org.cinchapi.concourse.server.upgrade.task;

import java.io.File;
import org.cinchapi.concourse.security.AccessManager;
import org.cinchapi.concourse.security.LegacyAccessManager;
import org.cinchapi.concourse.server.io.FileSystem;
import org.cinchapi.concourse.server.upgrade.SmartUpgradeTask;
import org.cinchapi.concourse.server.GlobalState;

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
        String accessFile = getServerInstallDirectory() 
                + File.separator + GlobalState.ACCESS_FILE;
        String accessBackupFile = getServerInstallDirectory() 
                + File.separator + ACCESS_FILE_BACKUP;
        if (FileSystem.hasFile(accessFile)) {
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
