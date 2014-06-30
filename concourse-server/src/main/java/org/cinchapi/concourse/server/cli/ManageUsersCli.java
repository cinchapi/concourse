/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2014 Jeff Nelson, Cinchapi Software Collective
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

import java.util.Arrays;

import org.cinchapi.concourse.server.jmx.ConcourseServerMXBean;

import com.beust.jcommander.Parameter;
import com.google.common.base.Strings;
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
                System.out
                        .println("WARNING: Option --grant is being deprecated,"
                                + " and replaced by options --add-user and --edit-user.");
                System.out.println("What is the username you want "
                        + "to add or modify?");
                byte[] username = console.readLine("").getBytes();
                System.out.println("What is the new password for this user?");
                byte[] password = console.readLine('*').getBytes();
                System.out.println("Re-enter password");
                byte[] reEnteredPassword = console.readLine('*').getBytes();
                if (Arrays.equals(password, reEnteredPassword)) {
                    bean.grant(username, password);
                    System.out.println("Consider it done.");
                }
                else {
                    throw new SecurityException("Not the same password. This" +
                    		" user has not been added or modified.");
                }
            }
            else if(opts.revoke) {
                System.out
                        .println("WARNING: Option --revoke is being deprecated,"
                                + " and replaced by option --delete-user.");
                System.out.println("What is the username you want to delete?");
                byte[] username = console.readLine("").getBytes();
                bean.revoke(username);
                System.out.println("Consider it done.");
            }
            else if(!Strings.isNullOrEmpty(opts.addingUsername)) {
                if(bean.hasUser(opts.addingUsername.getBytes())) {
                    console.readLine(opts.addingUsername + " already exists. "
                            + "Use CTRL-C to terminate or press RETURN to "
                            + "continue editing this user.");
                }
                if(Strings.isNullOrEmpty(opts.newPassword)) {
                    opts.newPassword = console.readLine("Password for "
                            + opts.addingUsername + " : ", '*');
                    String reEnteredPassword = console.readLine(
                            "Re-enter password : ", '*');
                    if (!opts.newPassword.equals(reEnteredPassword)) {
                        throw new SecurityException("Not the same password. This" +
                                " user has not been added.");
                    }
                }
                bean.grant(opts.addingUsername.getBytes(),
                        opts.newPassword.getBytes());
                System.out.println("Consider it done.");
            }
            else if(!Strings.isNullOrEmpty(opts.editingUsername)) {
                if(!bean.hasUser(opts.editingUsername.getBytes())) {
                    console.readLine(opts.editingUsername + " does not exist. "
                            + "Use CTRL-C to terminate or press RETURN to "
                            + "continue adding this user.");
                }
                if(Strings.isNullOrEmpty(opts.newPassword)) {
                    opts.newPassword = console.readLine("Password for "
                            + opts.editingUsername + " : ", '*');
                    String reEnteredPassword = console.readLine(
                            "Re-enter password : ", '*');
                    if (!opts.newPassword.equals(reEnteredPassword)) {
                        throw new SecurityException("Not the same password. This" +
                                " user has not been edited.");
                    }     
                }
                bean.grant(opts.editingUsername.getBytes(),
                        opts.newPassword.getBytes());
                System.out.println("Consider it done.");
            }
            else if(!Strings.isNullOrEmpty(opts.deletingUsername)) {
                if(!bean.hasUser(opts.deletingUsername.getBytes())) {
                    System.out.println(opts.deletingUsername
                            + " does not exist.");
                }
                else {
                    bean.revoke(opts.deletingUsername.getBytes());
                    System.out.println("Consider it done.");
                }
            }
            else {
                parser.usage();
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

        @Parameter(names = { "-g", "--grant" }, description = "[DEPRECATED] Add a new user or change the password for an existing user. ")
        public boolean grant = false;

        @Parameter(names = { "-r", "--revoke" }, description = "[DEPRECATED] Remove an existing user")
        public boolean revoke = false;

        @Parameter(names = { "-a", "--add-user" }, description = "Username of new user to add.")
        public String addingUsername;

        @Parameter(names = { "-e", "--edit-user" }, description = "Username of existing user to edit.")
        public String editingUsername;

        @Parameter(names = { "-d", "--delete-user" }, description = "Username of existing user to delete.")
        public String deletingUsername;

        @Parameter(names = { "-np", "--new-password" }, description = "Password of new user to add/edit.")
        public String newPassword;

    }

}
