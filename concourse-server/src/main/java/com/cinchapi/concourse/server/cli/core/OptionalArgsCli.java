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
package com.cinchapi.concourse.server.cli.core;

/**
 * A CLI that may require the presence of {@link #args} or make them optional.
 * 
 * @author Jeff Nelson
 */
public abstract class OptionalArgsCli extends ManagementCli {

    /**
     * Construct a new instance.
     * 
     * @param options
     * @param args
     */
    public OptionalArgsCli(Options options, String[] args) {
        super(options, args);
    }

    @Override
    protected boolean isReadyToRun() {
        return ((requireArgs() && !options.args.isEmpty()) || !requireArgs())
                && super.isReadyToRun();
    }

    /**
     * A flag that indicates whether the CLI requires one or more arguments in
     * order to run.
     * 
     * @return {@code true} or {@code false}
     */
    protected abstract boolean requireArgs();

}
