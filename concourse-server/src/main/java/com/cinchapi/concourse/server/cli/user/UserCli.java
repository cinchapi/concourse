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
package com.cinchapi.concourse.server.cli.user;

import com.beust.jcommander.Parameter;
import com.cinchapi.concourse.server.cli.core.OptionalArgsCli;
import com.cinchapi.concourse.server.cli.core.Options;
import com.google.common.base.CaseFormat;

/**
 * Marker class for CLIs that should be invokable from the main
 * {@link ManageUsersCli}.
 * 
 * @author Jeff Nelson
 */
public abstract class UserCli extends OptionalArgsCli {

    /**
     * Return the command that can be passed to the {@link ManageUsersCli} to
     * invoke this particular cli.
     * 
     * @return the command
     */
    public static String getCommand(Class<? extends UserCli> clazz) {
        return CaseFormat.UPPER_CAMEL
                .to(CaseFormat.LOWER_UNDERSCORE, clazz.getSimpleName())
                .split("_")[0];
    }

    /**
     * Construct a new instance.
     * 
     * @param options
     * @param args
     */
    public UserCli(Options options, String[] args) {
        super(options, args);
    }

    /**
     * Special options for the user cli.
     * 
     * @author Jeff Nelson
     */
    protected static class UserOptions extends Options {

        @Parameter(names = {
                "--set-password" }, description = "Password of the user that is being managed (e.g. the non-invoking user)")
        public String userPassword;
    }
}
