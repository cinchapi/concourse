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
package com.cinchapi.concourse.server.cli.user;

import java.nio.ByteBuffer;

import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.common.io.ByteBuffers;
import com.cinchapi.concourse.server.cli.core.CommandLineInterfaceInformation;
import com.cinchapi.concourse.server.management.ConcourseManagementService.Client;

/**
 * A CLI for granting user permissions.
 *
 * @author Jeff Nelson
 */
@CommandLineInterfaceInformation(description = "Grant permissions to a user")
public class GrantUserCli extends UserCli {

    public GrantUserCli(String[] args) {
        super(new GrantPermissionOptions(), args);
    }

    @Override
    protected boolean requireArgs() {
        return false;
    }

    @Override
    protected void doTask(Client client) {
        try {
            GrantPermissionOptions opts = (GrantPermissionOptions) options;
            String username;
            if(opts.args.isEmpty()) {
                username = console.readLine(
                        "To which user do you want to grant a permission? ");
            }
            else {
                username = opts.args.get(0);
            }
            ByteBuffer uname = ByteBuffers.fromUtf8String(username);
            client.grant(uname, opts.permission, opts.environment, token);
            System.out.println(
                    AnyStrings.format("Sucessfully granted {} in {} to {}",
                            opts.permission, opts.environment, username));

        }
        catch (Exception e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }

    }

}
