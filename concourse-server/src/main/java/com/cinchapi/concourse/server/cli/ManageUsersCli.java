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

import com.beust.jcommander.Parameter;
import com.cinchapi.concourse.server.jmx.ConcourseServerMXBean;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;

/**
 * A management CLI to add/modify/remove user access to the server.
 * 
 * @author Jeff Nelson
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
                bean.grant(username, password);
                System.out.println("Consider it done.");
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
            else if(opts.listSessions) {
                System.out.println("Current User Sessions:");
                System.out.println(bean.listAllUserSessions());
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
                    if(!opts.newPassword.equals(reEnteredPassword)) {
                        throw new SecurityException(
                                "Not the same password. This"
                                        + " user has not been added.");
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
                    if(!opts.newPassword.equals(reEnteredPassword)) {
                        throw new SecurityException(
                                "Not the same password. This"
                                        + " user has not been edited.");
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

    @Override
    protected boolean isReadyToRun() {
        MyOptions opts = (MyOptions) options;
        return super.isReadyToRun()
                && (opts.grant || opts.revoke || opts.listSessions
                        || !Strings.isNullOrEmpty(opts.addingUsername)
                        || !Strings.isNullOrEmpty(opts.deletingUsername) || !Strings
                            .isNullOrEmpty(opts.editingUsername));
    }

    /**
     * The options that can be passed to the main method of this script.
     * 
     * @author Jeff Nelson
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

        @Parameter(names = { "--list-sessions" }, description = "List the user sessions that are currently active")
        public boolean listSessions = false;

    }

}
