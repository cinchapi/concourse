/*
 * Copyright (c) 2013-2016 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse.server.cli;

import java.io.IOException;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import jline.console.ConsoleReader;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.cinchapi.concourse.server.ConcourseServer;
import com.cinchapi.concourse.server.jmx.ConcourseServerMXBean;
import com.google.common.base.CaseFormat;
import com.google.common.base.Strings;

/**
 * A CLI that performs an operation on a {@link ConcourseServerMXBean}. Any CLI
 * that operates on a running {@link ConcourseServer} should extend this class.
 * 
 * @author Jeff Nelson
 */
public abstract class ManagedOperationCli {

    /**
     * The CLI options.
     */
    protected Options options;

    /**
     * The parser that validates the CLI options.
     */
    protected JCommander parser;

    /**
     * Handler to the console for interactive I/O.
     */
    protected ConsoleReader console;

    /**
     * Construct a new instance that is seeded with an object containing options
     * metadata. The {@code options} will be parsed by {@link JCommander} to
     * configure them appropriately.
     * 
     * @param options
     * @param args - these usually come from the main method
     */
    public ManagedOperationCli(Options options, String... args) {
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
            MBeanServerConnection connection = JMXConnectorFactory.connect(
                    new JMXServiceURL(ConcourseServerMXBean.JMX_SERVICE_URL))
                    .getMBeanServerConnection();
            ObjectName objectName = new ObjectName(
                    "com.cinchapi.concourse.server.jmx:type=ConcourseServerMXBean");
            ConcourseServerMXBean bean = JMX.newMBeanProxy(connection,
                    objectName, ConcourseServerMXBean.class);
            if(Strings.isNullOrEmpty(options.password)) {
                options.password = console.readLine("password for ["
                        + options.username + "]: ", '*');
            }
            byte[] username = options.username.getBytes();
            byte[] password = options.password.getBytes();
            if(bean.login(username, password)) {
                doTask(bean);
                System.exit(0);
            }
            else {
                die("Invalid username/password combination.");
            }
        }
        catch (IOException e) {
            die("Could not connect to the management server. Please check "
                    + "that ConcourseServer is running with JMX enabled.");
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
        System.exit(2);
    }
    
    /**
     * Return {@code true} if the managed task has sufficient conditions
     * to run.
     * 
     * @return {@code true} if the managed task has sufficient conditions
     * to run
     */
    protected boolean isReadyToRun() {
        return !options.help;
    }

    /**
     * Implement a managed task that involves at least one of the operations
     * available from {@code bean}. This method is called by the main
     * {@link #run()} method, so the implementer should place all task logic
     * here.
     * <p>
     * DO NOT call {@link System#exit(int)} with '0' from this method
     * </p>
     * 
     * @param bean
     */
    protected abstract void doTask(ConcourseServerMXBean bean);

}
