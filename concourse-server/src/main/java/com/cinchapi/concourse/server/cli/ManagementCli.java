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
package com.cinchapi.concourse.server.cli;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.annotation.Nullable;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import jline.console.ConsoleReader;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.cinchapi.concourse.server.ConcourseServer;
import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.management.ConcourseManagementService.Client;
import com.cinchapi.concourse.thrift.AccessToken;
import com.google.common.base.CaseFormat;
import com.google.common.base.Strings;

/**
 * A CLI that performs an operation on a {@link ConcourseServerMXBean}. Any CLI
 * that operates on a running {@link ConcourseServer} should extend this class.
 * <p>
 * Unlike children of {@code com.cinchapi.concourse.cli.CommandLineInterface},
 * CLIs that extend this class are assumed to live within the Concourse Server
 * deployment in the standard directory for CLIs.
 * </p>
 * 
 * @author Jeff Nelson
 */
public abstract class ManagementCli {

    /**
     * The host where the management server is located.
     */
    private static String MANAGEMENT_SERVER_HOST = "localhost";

    /**
     * Handler to the console for interactive I/O.
     */
    protected ConsoleReader console;

    /**
     * The CLI options.
     */
    protected Options options;

    /**
     * The parser that validates the CLI options.
     */
    protected JCommander parser;

    /**
     * {@link AccessToken} access token for management server
     */
    protected AccessToken token;

    /**
     * A socket that is opened to the management server in the {@link #run()}
     * method.
     */
    @Nullable
    private TSocket socket;

    @Nullable
    private Client client;

    /**
     * Construct a new instance that is seeded with an object containing options
     * metadata. The {@code options} will be parsed by {@link JCommander} to
     * configure them appropriately.
     * 
     * @param options
     * @param args - these usually come from the main method
     */
    public ManagementCli(Options options, String... args) {
        try {
            this.parser = new JCommander(options, args);
            this.options = options;
            parser.setProgramName(CaseFormat.UPPER_CAMEL.to(
                    CaseFormat.LOWER_HYPHEN, this.getClass().getSimpleName()));
            if(!isReadyToRun()) {
                parser.usage();
                System.exit(1);
            }
            this.console = new ConsoleReader();
            console.setExpandEvents(false);
        }
        catch (ParameterException e) {
            die(e.getMessage());
        }
        catch (IOException e) {
            die(e.getMessage());
        }
    }

    /**
     * Run the CLI. This method should only be called from the main method.
     */
    public final void run() {
        try {
            socket = new TSocket(MANAGEMENT_SERVER_HOST,
                    GlobalState.MANAGEMENT_PORT);
            socket.open();
            client = new Client(new TBinaryProtocol(socket));
            if(Strings.isNullOrEmpty(options.password)) {
                options.password = console.readLine(
                        "password for [" + options.username + "]: ", '*');
            }
            token = client.login(ByteBuffer.wrap(options.username.getBytes()),
                    ByteBuffer.wrap(options.password.getBytes()));
            doTask(client);
            exit(0);
        }
        catch (TTransportException e) {
            die("Could not connect to the management server. Please check "
                    + "that Concourse Server is running.");
        }
        catch (Exception e) {
            die(e.getMessage());
        }
    }

    /**
     * Print {@code message} to stderr and exit with a non-zero status.
     * 
     * @param message
     */
    protected void die(String message) {
        System.err.println("ERROR: " + message);
        exit(2);
    }

    /**
     * Implement a managed task that involves at least one of the operations
     * available from {@code client}. This method is called by the main
     * {@link #run()} method, so the implementer should place all task logic
     * here.
     * <p>
     * DO NOT call {@link System#exit(int)} with '0' from this method
     * </p>
     * 
     * @param client
     */
    protected abstract void doTask(Client client);

    /**
     * Return the host Concourse Server's "home" directory.
     * 
     * @return the host application's home
     */
    @Nullable
    protected final Path getServerHome() {
        String path = System.getProperty("user.app.home"); // this is set by the
                                                           // .env script that
                                                           // is sourced by
                                                           // every server-side
                                                           // CLI
        return path != null ? Paths.get(path) : null;
    }

    /**
     * Return the original working directory from which the CLI was launched.
     * This information is sometimes necessary to properly resolve file paths.
     * 
     * @return the launch directory or {@code null} if the CLI is unable to
     *         determine its original working directory
     */
    @Nullable
    protected final String getLaunchDirectory() {
        return System.getProperty("user.dir.real"); // this is set by the .env
                                                    // script that is sourced by
                                                    // every server-side CLI
    }

    /**
     * Return {@code true} if the managed task has sufficient conditions
     * to run.
     * 
     * @return {@code true} if the managed task has sufficient conditions
     *         to run
     */
    protected boolean isReadyToRun() {
        return !options.help;
    }

    /**
     * Clean up resources and issue a {@link System#exit(int)} with the
     * specified {@code status}.
     * 
     * @param status the exit status
     */
    private void exit(int status) {
        if(client != null) {
            try {
                client.logout(token);
            }
            catch (TException e) {
                e.printStackTrace();
            }
        }
        if(socket != null) {
            socket.close();
        }
        System.exit(status);
    }

}
