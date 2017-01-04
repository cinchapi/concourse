/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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

import java.lang.reflect.Constructor;
import java.text.MessageFormat;
import java.util.Arrays;

/**
 * A single runner that can execute any {@link CommandLineInterface} class that
 * is specified as the first argument. This class is designed to make it easier
 * to create CLI applications.
 * <p>
 * <ol>
 * <li>Write a CLI class that extends {@link CommandLineInterface}. The CLI must
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
 */
@SuppressWarnings("unchecked")
public final class CommandLineInterfaceRunner {

    /**
     * Run the appropriate CLI program
     * 
     * @param args
     */
    public static void main(String... args) {
        if(args.length == 0) {
            System.err.println("ERROR: Please specify a "
                    + "CommandLineInterface to run");
            System.exit(1);
        }
        String name = args[0];
        args = Arrays.copyOfRange(args, 1, args.length);
        Constructor<? extends CommandLineInterface> constructor = null;
        try {
            Class<?> clazz = Class.forName(name);
            constructor = (Constructor<? extends CommandLineInterface>) clazz
                    .getConstructor(String[].class);
        }
        catch (Exception e) {
            System.err.println(MessageFormat.format(
                    "Cannot execute CommandLineInterface named {0}", name));
            e.printStackTrace();
            System.exit(1);
        }
        // Not worried about NPE because #constructor is guaranteed to be
        // initialized if we make it this far
        try {
            CommandLineInterface cli = constructor.newInstance((Object) args);
            System.exit(cli.run());
        }
        catch (Exception e) {
            // At this point, the Exception is thrown from the CLI (i.e. the
            // user did not pass in a required arg, etc).
            if(e instanceof ReflectiveOperationException
                    && e.getCause().getMessage() != null) {
                System.err.println(MessageFormat.format("ERROR: {0}", e
                        .getCause().getMessage()));
            }
            System.exit(1);
        }

    }

    /**
     * A hook to run {@link CommandLineInterface clis} programmatically.
     * 
     * @param clazz the CLI class
     * @param flags the flags to pass to the cli, formatted the same as they
     *            would be on the command line
     */
    public static void run(Class<? extends CommandLineInterface> clazz,
            String flags) {
        String[] args0 = flags.split("\\s");
        String[] args = new String[args0.length + 1];
        args[0] = clazz.getName();
        System.arraycopy(args0, 0, args, 1, args0.length);
        main(args);
    }

    private CommandLineInterfaceRunner() {}
}
