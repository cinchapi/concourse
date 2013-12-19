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

import org.cinchapi.concourse.server.jmx.ConcourseServerMXBean;
import com.beust.jcommander.Parameter;
import com.google.common.base.Throwables;

/**
 * A management CLI to add/modify/remove user access to the server.
 * 
 * @author jnelson
 */
public class ManageUsersCli extends ManagedOperationCli {

    /**
     * Run the program...
     * 
     * @param args
     */
    public static void main(String... args) {
        ManageUsersCli cli = new ManageUsersCli(args);
        cli.run();
    }

    /**
     * Construct a new instance.
     * 
     * @param options
     * @param args
     */
    public ManageUsersCli(String[] args) {
        super(new MyOptions(), args);
    }

    @Override
    protected void doTask(ConcourseServerMXBean bean) {
        MyOptions opts = (MyOptions) options;
        try {
            if(opts.grant) {
                System.out.println("What is the username you want "
                        + "to add or modify?");
                byte[] username = console.readLine("").getBytes();
                System.out.println("What is the new password for this user?");
                byte[] password = console.readLine('*').getBytes();
                bean.grant(username, password);
                System.out.println("Consider it done.");
            }
            else if(opts.revoke) {
                System.out.println("What is the username you want to delete?");
                byte[] username = console.readLine("").getBytes();
                bean.revoke(username);
                System.out.println("Consider it done.");
            }
            else {
                die("Please specify either the --grant " + "or --revoke option");
            }
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * The options that can be passed to the main method of this script.
     * 
     * @author jnelson
     */
    private static class MyOptions extends Options {

        @Parameter(names = { "-g", "--grant" }, description = "Add a new user or change the password for an existing user.")
        public boolean grant = false;

        @Parameter(names = { "-r", "--revoke" }, description = "Remove an existing user")
        public boolean revoke = false;

    }

}
