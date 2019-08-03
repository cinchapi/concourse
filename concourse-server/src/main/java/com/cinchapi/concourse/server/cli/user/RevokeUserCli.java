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
@CommandLineInterfaceInformation(description = "Revoke permissions from a user")
public class RevokeUserCli extends UserCli {

    public RevokeUserCli(String[] args) {
        super(new PermissionOptions(), args);
    }

    @Override
    protected boolean requireArgs() {
        return false;
    }

    @Override
    protected void doTask(Client client) {
        try {
            PermissionOptions opts = (PermissionOptions) options;
            String username;
            if(opts.args.isEmpty()) {
                username = console.readLine(
                        "From which user do you want to revoke permissions? ");
            }
            else {
                username = opts.args.get(0);
            }
            ByteBuffer uname = ByteBuffers.fromUtf8String(username);
            client.revoke(uname, opts.environment, token);
            System.out.println(AnyStrings.format(
                    "Sucessfully revoked all permissions for {} in {}",
                    username, opts.environment));

        }
        catch (Exception e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }

    }

}
