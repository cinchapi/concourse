/*
 * Copyright (c) 2013-2023 Cinchapi Inc.
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

/**
 * A single runner that can execute any
 * {@link ConcourseCommandLineInterface} class that
 * is specified as the first argument. This class is designed to make it easier
 * to create CLI applications.
 * <p>
 * <ol>
 * <li>Write a CLI class that extends
 * {@link ConcourseCommandLineInterface}. The CLI must
 * define a public constructor that takes a single array of string arguments
 * (i.e. similar to a main method).</li>
 * <li>Write a wrapper script that invokes
 * <code>com.cinchapi.concourse.cli.CommandLineInterfaceRunner &lt;cli&gt; &lt;cli-args&gt;</code>
 * where {@code cli} is the fully name of the CLI class from step 1 and
 * {@code cli-args} are the arguments that should be passed to tht CLI</li>
 * <p>
 * <strong>NOTE:</strong> Be sure to add both CommandLineInterfaceRunner and the
 * CLI class from step 1 to the classpath you feed to the JVM in the shell
 * script.
 * </p>
 * </ol>
 * </p>
 * 
 * @author Jeff Nelson
 * @deprecated use {@link com.cinchapi.lib.cli.CommandLineInterfaceRunner}
 *             instead.
 */
@Deprecated
public final class CommandLineInterfaceRunner {

    /**
     * Run the appropriate CLI program
     * 
     * @param args
     */
    public static void main(String... args) {
        com.cinchapi.lib.cli.CommandLineInterfaceRunner.main(args);
    }

    /**
     * A hook to run {@link ConcourseCommandLineInterface clis}
     * programmatically.
     * 
     * @param clazz the CLI class
     * @param flags the flags to pass to the cli, formatted the same as they
     *            would be on the command line
     */
    public static void run(Class<? extends ConcourseCommandLineInterface> clazz,
            String flags) {
        com.cinchapi.lib.cli.CommandLineInterfaceRunner.run(clazz, flags);
    }

    private CommandLineInterfaceRunner() {}
}
