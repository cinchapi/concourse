/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013 Jeff Nelson, Cinchapi Software Collective
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.cinchapi.concourse.server.cli;

import java.io.IOException;
import java.net.MalformedURLException;
import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import jline.console.ConsoleReader;

import org.apache.thrift.TException;
import org.cinchapi.concourse.server.ConcourseServer;
import org.cinchapi.concourse.server.jmx.ConcourseServerMXBean;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.common.base.CaseFormat;

/**
 * A CLI that performs an operation on a {@link ConcourseServerMXBean}. Any CLI
 * that operates on a running {@link ConcourseServer} should extend this class.
 * 
 * @author jnelson
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
     * metadata. The {@code opts} will be parsed by {@link JCommander} so
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
            this.console = new ConsoleReader();
        }
        catch (ParameterException e) {
            System.err.println(e.getMessage());
            System.exit(2);
        }
        catch (IOException e) {
            System.err.println(e.getMessage());
            System.exit(2);
        }
    }

    /**
     * Run the CLI. This method should only be called from the main method.
     * 
     * @throws MalformedObjectNameException
     * @throws MalformedURLException
     * @throws IOException
     * @throws TException
     */
    public final void run() throws Exception {
        MBeanServerConnection connection = JMXConnectorFactory.connect(
                new JMXServiceURL(ConcourseServerMXBean.JMX_SERVICE_URL))
                .getMBeanServerConnection();
        ObjectName objectName = new ObjectName(
                "org.cinchapi.concourse.server.jmx:type=ConcourseServerMXBean");
        ConcourseServerMXBean bean = JMX.newMBeanProxy(connection, objectName,
                ConcourseServerMXBean.class);
        if(options.help) {
            parser.usage();
            System.exit(1);
        }
        else {
            doTask(bean);
            System.exit(0);
        }
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
