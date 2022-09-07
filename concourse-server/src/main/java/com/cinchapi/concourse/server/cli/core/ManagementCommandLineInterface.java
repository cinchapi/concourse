/*
 * Copyright (c) 2013-2022 Cinchapi Inc.
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

import java.nio.ByteBuffer;

import javax.annotation.Nullable;

import jline.console.ConsoleReader;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import com.beust.jcommander.JCommander;
import com.cinchapi.concourse.server.ConcourseServer;
import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.management.ConcourseManagementService.Client;
import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.lib.cli.CommandLineInterface;
import com.cinchapi.lib.cli.Options;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;

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
public abstract class ManagementCommandLineInterface
        extends CommandLineInterface {

    /**
     * The host where the management server is located.
     */
    private static String MANAGEMENT_SERVER_HOST = "localhost";

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

    /**
     * The Management client.
     */
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
    public ManagementCommandLineInterface(String... args) {
        super(args);
    }

    @Override
    protected final void doTask() {
        doTask(client);
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

    @Override
    protected void setup(Options options, ConsoleReader console) {
        try {
            socket = new TSocket(MANAGEMENT_SERVER_HOST,
                    GlobalState.MANAGEMENT_PORT);
            socket.open();
            client = new Client(new TBinaryProtocol(socket));
            ManagementOptions opts = (ManagementOptions) options;
            if(Strings.isNullOrEmpty(opts.password)) {
                opts.password = console.readLine(
                        "password for [" + opts.username + "]: ", '*');
            }
            token = client.login(ByteBuffer.wrap(opts.username.getBytes()),
                    ByteBuffer.wrap(opts.password.getBytes()));
        }
        catch (TTransportException e) {
            halt("Could not connect to the management server. Please check "
                    + "that Concourse Server is running.");
        }
        catch (Exception e) {
            Throwable cause = Throwables.getRootCause(e);
            halt(cause.getMessage());
        }
    }

    @Override
    protected void teardown() {
        if(client != null && token != null) {
            try {
                client.logout(token);
            }
            catch (com.cinchapi.concourse.thrift.SecurityException e) {
                // CON-590: The token has been invalidated, but we can ignore
                // it at this point since the work that requires authorization
                // has already been done.
            }
            catch (TException e) {
                e.printStackTrace();
            }
        }
        if(socket != null) {
            socket.close();
        }
    }

    @Override
    protected abstract ManagementOptions getOptions();

}
