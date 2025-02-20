/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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
package com.cinchapi.concourse.cli;

import java.nio.file.Paths;

import jline.console.ConsoleReader;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.config.ConcourseClientConfiguration;
import com.cinchapi.concourse.util.FileOps;
import com.cinchapi.lib.cli.Options;
import com.google.common.base.Strings;

/**
 * A {@link ConcourseCommandLineInterface} is a console tool that
 * interacts with {@link Concourse} via the client interface. This class
 * contains boilerplate logic for grabbing authentication credentials and
 * establishing a connection to Concourse.
 * 
 * @author Jeff Nelson
 */
public abstract class ConcourseCommandLineInterface
        extends com.cinchapi.lib.cli.CommandLineInterface {

    /**
     * Internal {@link Concourse} instance that should be used to do the work of
     * the CLI.
     */
    protected Concourse concourse;

    /**
     * Construct a new instance.
     * 
     * @param args
     */
    protected ConcourseCommandLineInterface(String... args) {
        super(args);
    }

    @Override
    protected void setup(Options options, ConsoleReader console) {
        try {
            ConcourseOptions opts = (ConcourseOptions) options;
            if(!Strings.isNullOrEmpty(opts.config)) {
                opts.config = FileOps.expandPath(opts.config,
                        getLaunchDirectory());
                opts.prefs = opts.config; // backwards compatibility
                ConcourseClientConfiguration config = ConcourseClientConfiguration
                        .from(Paths.get(opts.config));
                opts.username = config.getUsername();
                opts.password = new String(config.getPasswordExplicit());
                opts.host = config.getHost();
                opts.port = config.getPort();
                opts.environment = config.getEnvironment();
            }
            if(Strings.isNullOrEmpty(opts.password)) {
                opts.password = console.readLine(
                        "password for [" + opts.username + "]: ", '*');
            }
            int attemptsRemaining = 5;
            while (concourse == null && attemptsRemaining > 0) {
                try {
                    concourse = Concourse.connect(opts.host, opts.port,
                            opts.username, opts.password, opts.environment);
                }
                catch (Exception e) {
                    System.err.println("Error processing login. Please check "
                            + "username/password combination and try again.");
                    concourse = null;
                    opts.password = console.readLine(
                            "password for [" + opts.username + "]: ", '*');
                    attemptsRemaining--;
                }
            }
            if(concourse == null) {
                throw new IllegalStateException(
                        "Unable to connect to Concourse. Is the server running?");
            }
        }
        catch (Exception e) {
            throw CheckedExceptions.throwAsRuntimeException(e);
        }
    }

    @Override
    protected abstract ConcourseOptions getOptions();

}