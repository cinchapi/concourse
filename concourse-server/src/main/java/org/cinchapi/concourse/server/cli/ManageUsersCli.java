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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.cinchapi.concourse.server.jmx.ConcourseServerMXBean;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
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
            boolean addingUser = !Strings.isNullOrEmpty(opts.addingUsername);
            boolean editingUser = !Strings.isNullOrEmpty(opts.editingUsername);
            boolean deletingUser = !Strings.isNullOrEmpty(opts.deletingUsername);           
            if((addingUser && editingUser) || (addingUser && deletingUser)
                    || (editingUser && deletingUser)) {
                die("You can only do one action at a time.");
            }          
            while (!addingUser && !editingUser && !deletingUser)  {
                String action = console.readLine(
                        "Specify action [add | edit | del | exit]: ").trim();
                if("add".equalsIgnoreCase(action)) {
                    addingUser = true;
                }
                else if("edit".equalsIgnoreCase(action)) {
                    editingUser = true;
                }
                else if("del".equalsIgnoreCase(action)) {
                    deletingUser = true;
                }
                else if("exit".equalsIgnoreCase(action)) {
                    System.exit(1);
                }
                else {
                    System.out.println(action + " action does not exist.");
                }
            }
            if(addingUser) {
                doAddNewUserTask(opts.addingUsername, opts.newPassword, bean);
            }
            else if(editingUser) {
                doEditUserTask(opts.editingUsername, opts.newPassword, bean);
            }
            else if(deletingUser) {
                doDeleteUserTask(opts.deletingUsername, bean);
            }  
            System.out.println("Consider it done.");
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

        @Parameter(names = { "-a", "--adduser" }, validateWith = UsernameValidator.class,
                description = "Username of new user to add.")
        public String addingUsername;
        
        @Parameter(names = { "-e", "--edituser" }, validateWith = UsernameValidator.class,
                description = "Username of existing user to edit.")
        public String editingUsername;
        
        @Parameter(names = { "-d", "--deluser" }, validateWith = UsernameValidator.class,
                description = "Username of existing user to delete.")
        public String deletingUsername;
        
        @Parameter(names = { "-np", "--new-password" }, validateWith = PasswordValidator.class,
                description = "Password of new user to add/edit.")
        public String newPassword;
        
    }
    
    /**
     * The validator to validate parsed username from command line.
     * 
     * @author knd
     */
    public static class UsernameValidator implements IParameterValidator {
        
        public String validationErrorMsg = "Username cannot be empty " +
        		"or contain whitespaces.";
        
        @Override
        public void validate(String name, String value) throws ParameterException { 
            if(Strings.isNullOrEmpty(value)) {
                throw new ParameterException(validationErrorMsg);
            }
            Matcher matcher = Pattern.compile("\\s").matcher(value);
            boolean hasWhiteSpace = matcher.find();
            if (hasWhiteSpace) {
                throw new ParameterException(validationErrorMsg);
            }
        }
        
        /**
         * Checks if the username is valid.
         * 
         * @param value
         * @return true/false
         */
        public boolean isValidUsername(String value) {
            try {
                validate(null, value);
                return true;
            }
            catch (Exception e) {
                return false;
            }
        }
        
    }
    
    /**
     * The validator to validate parsed password from command line.
     * 
     * @author knd
     */
    public static class PasswordValidator implements IParameterValidator {
        
        public String validationErrorMsg = "Password " +
        		"cannot be empty, or have fewer than 3 characters, " +
        		"or contain whitespaces.";
        
        @Override
        public void validate(String name, String value) throws ParameterException {
            if(Strings.isNullOrEmpty(value)) {
                throw new ParameterException(validationErrorMsg);
            }
            else if (value.length() < 3) {
                throw new ParameterException(validationErrorMsg);
            }
            Matcher matcher = Pattern.compile("\\s").matcher(value);
            boolean hasWhiteSpace = matcher.find();
            if (hasWhiteSpace) {
                throw new ParameterException(validationErrorMsg);
            }
        }
        
        /**
         * Check if the password is valid.
         * 
         * @param value
         * @return true/false
         */
        public boolean isValidPassword(String value) {
            try {
                validate(null, value);
                return true;
            }
            catch (Exception e) {
                return false;
            }
        }
        
    }
    
    /**
     * Execute the task of adding new user with {@code newUsername}
     * and {@code newPassword} to {@code bean} server.
     * 
     * @param newUsername
     * @param newPassword
     * @param bean
     * @throws Exception
     */
    private void doAddNewUserTask(String newUsername, String newPassword,
            ConcourseServerMXBean bean) throws Exception {
        UsernameValidator userValidator = new UsernameValidator();
        PasswordValidator passwordValidator = new PasswordValidator();
        boolean validUsername = false;    
        if(!Strings.isNullOrEmpty(newUsername)) {
            // if in here, which means newUsername passes 
            // the validator of JCommander parser
            if(!bean.hasUser(newUsername.getBytes())) {
                validUsername = true;
            }
            else {
                System.out.println(newUsername + " already exists.");
            }
        }
        while (!validUsername) {
            String usernameInput = console.readLine("Username you want to add: ");
            if (!userValidator.isValidUsername(usernameInput)) {
                System.out.println(userValidator.validationErrorMsg);
                continue;
            }        
            if (!bean.hasUser(usernameInput.getBytes())) {
                newUsername = usernameInput;
                validUsername = true;
            }
            else {
                System.out.println(usernameInput + " already exists.");
            }
        } 
        while (Strings.isNullOrEmpty(newPassword)) {
            String passwordInput = console.readLine(
                    "Password for " + newUsername + " : ", '*');
            if (!passwordValidator.isValidPassword(passwordInput)) {
                System.out.println(passwordValidator.validationErrorMsg);
            }
            else {
                newPassword = passwordInput;
            }      
        }
        bean.grant(newUsername.getBytes(), newPassword.getBytes());
    }
    
    /**
     * Execute the task of editing existing user by {@code username}
     * with {@code newPassword} to {@code bean} server.
     * 
     * @param username
     * @param newPassword
     * @param bean
     * @throws Exception
     */
    private void doEditUserTask(String username, String newPassword,
            ConcourseServerMXBean bean) throws Exception {
        UsernameValidator userValidator = new UsernameValidator();
        PasswordValidator passwordValidator = new PasswordValidator();
        boolean validUsername = false;     
        if (!Strings.isNullOrEmpty(username)) {
            // if in here, which means newUsername passes 
            // the validator of JCommander parser
            if (bean.hasUser(username.getBytes())) {
                validUsername = true;
            }
            else {
                System.out.println(username + " does not exist.");
            }
        }
        while (!validUsername) {
            String usernameInput = console.readLine("Username you want to edit: ");
            if (!userValidator.isValidUsername(usernameInput)) {
                System.out.println(userValidator.validationErrorMsg);
                continue;
            }        
            if (bean.hasUser(usernameInput.getBytes())) {
                username = usernameInput;
                validUsername = true;
            }
            else {
                System.out.println(usernameInput + " does not exist.");
            }
        } 
        while (Strings.isNullOrEmpty(newPassword)) {
            String passwordInput = console.readLine(
                    "Password for " + username + " : " , '*');
            if (!passwordValidator.isValidPassword(passwordInput)) {
                System.out.println(passwordValidator.validationErrorMsg);
            }
            else {
                newPassword = passwordInput;
            }      
        }
        bean.grant(username.getBytes(), newPassword.getBytes());
    }
    
    /**
     * Execute the task of deleting existing user by {@code username}
     * to {@code bean} server.
     * 
     * @param username
     * @param bean
     * @throws Exception
     */
    private void doDeleteUserTask(String username, ConcourseServerMXBean bean)
            throws Exception {
        UsernameValidator userValidator = new UsernameValidator();
        boolean validUsername = false;
        if (!Strings.isNullOrEmpty(username)) {
            // if in here, which means newUsername passes 
            // the validator of JCommander parser
            if (bean.hasUser(username.getBytes())) {
                validUsername = true;
            }
            else {
                System.out.println(username + " does not exist.");
            }
        }
        while (!validUsername) {
            String usernameInput = console.readLine("Username you want to edit: ");
            if (!userValidator.isValidUsername(usernameInput)) {
                System.out.println(userValidator.validationErrorMsg);
                continue;
            }        
            if (bean.hasUser(usernameInput.getBytes())) {
                username = usernameInput;
                validUsername = true;
            }
            else {
                System.out.println(usernameInput + " does not exist.");
            }
        }
        bean.revoke(username.getBytes());
    }

}
