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
package com.cinchapi.concourse.server.cli.env;

import com.cinchapi.concourse.server.cli.core.OptionalArgsCli;
import com.cinchapi.concourse.server.cli.core.Options;
import com.google.common.base.CaseFormat;

/**
 * Marker class for CLIs that should be invokable from the main
 * {@link ManageEnvironmentsCli}.
 * 
 * @author Jeff Nelson
 */
public abstract class EnvironmentCli extends OptionalArgsCli {

    /**
     * Return the command that can be passed to the
     * {@link ManageEnvironmentsCli} to
     * invoke this particular cli.
     * 
     * @return the command
     */
    public static String getCommand(Class<? extends EnvironmentCli> clazz) {
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
    public EnvironmentCli(Options options, String[] args) {
        super(options, args);
    }

    /**
     * Special options for the environment cli.
     * 
     * @author Jeff Nelson
     */
    protected static class EnvironmentOptions extends Options {} // NOTE: not
                                                                 // extending
                                                                 // EnvironmentOptions
                                                                 // because any
                                                                 // management
                                                                 // options that
                                                                 // we take on
                                                                 // an
                                                                 // environment
                                                                 // (i.e.
                                                                 // delete)
                                                                 // should be
                                                                 // specified as
                                                                 // --delete
                                                                 // <env>
                                                                 // instead of
                                                                 // --delete -e
                                                                 // <env>

}
