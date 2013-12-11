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
package org.cinchapi.concourse.shell;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;

import groovy.lang.Binding;
import groovy.lang.Closure;
import groovy.lang.GroovyShell;

import jline.TerminalFactory;
import jline.console.ConsoleReader;
import jline.console.completer.StringsCompleter;

import org.apache.thrift.transport.TTransportException;
import org.cinchapi.concourse.Concourse;
import org.cinchapi.concourse.thrift.Operator;
import org.joda.time.DateTime;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.clutch.dates.StringToTime;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.primitives.Longs;

/**
 * The main program runner for the ConcourseShell client. ConcourseShell wraps a
 * connection to a ConcourseServer inside of a {@link GroovyShell}, which allows
 * for rich interaction with Concourse in a scripting environment.
 * 
 * @author jnelson
 */
public final class ConcourseShell {

    /**
     * The text that is displayed when the user requests HELP.
     */
    private static String HELP_TEXT = "";
    static {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(
                    ConcourseShell.class.getResourceAsStream("/man")));
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.replaceAll("\"", "\\\\\"");
                HELP_TEXT += line + System.getProperty("line.separator");
            }
            HELP_TEXT = HELP_TEXT.trim();
            reader.close();
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * A closure that converts a string description to a time.
     */
    private static Closure<DateTime> STRING_TO_TIME = new Closure<DateTime>(
            null) {

        private static final long serialVersionUID = 1L;

        @Override
        public DateTime call(Object arg) {
            if(Longs.tryParse(arg.toString()) != null) {
                // We should assume that the timestamp is in microseconds since
                // that is the output format used in ConcourseShell
                return new DateTime(TimeUnit.MILLISECONDS.convert(
                        Long.parseLong(arg.toString()), TimeUnit.MICROSECONDS));
            }
            else {
                return StringToTime.parseDateTime(arg.toString());
            }
        }

    };

    /**
     * Run the program...
     * 
     * @param args - see {@link Options}
     * @throws IOException
     */
    public static void main(String... args) throws IOException {
        ConsoleReader console = new ConsoleReader();
        Options opts = new Options();
        JCommander parser = new JCommander(opts, args);
        parser.setProgramName("concourse-shell");
        if(opts.help) {
            parser.usage();
            System.exit(1);
        }
        if(Strings.isNullOrEmpty(opts.password)) {
            opts.password = console.readLine("Password: ", '*');
        }
        try {
            Concourse concourse = new Concourse.Client(opts.host, opts.port,
                    opts.username, opts.password);

            CommandLine.displayWelcomeBanner();
            Binding binding = new Binding();
            GroovyShell shell = new GroovyShell(binding);

            Stopwatch watch = Stopwatch.createUnstarted();

            console.println("Use HELP for help.");
            console.setPrompt("cash$ ");
            console.addCompleter(new StringsCompleter("concourse.add",
                    "concourse.audit", "concourse.clear", "concourse.create",
                    "concourse.describe", "concourse.fetch", "concourse.find",
                    "concourse.get", "concourse.link", "concourse.ping",
                    "concourse.remove", "concourse.revert", "concourse.search",
                    "concourse.set", "concourse.unlink", "concourse.verify"));

            String line;
            while ((line = console.readLine().trim()) != null) {
                binding.setVariable("concourse", concourse);
                binding.setVariable("eq", Operator.EQUALS);
                binding.setVariable("ne", Operator.NOT_EQUALS);
                binding.setVariable("gt", Operator.GREATER_THAN);
                binding.setVariable("gte", Operator.GREATER_THAN_OR_EQUALS);
                binding.setVariable("lt", Operator.LESS_THAN);
                binding.setVariable("lte", Operator.LESS_THAN_OR_EQUALS);
                binding.setVariable("bw", Operator.BETWEEN);
                binding.setVariable("regex", Operator.REGEX);
                binding.setVariable("nregex", Operator.NOT_REGEX);
                binding.setVariable("date", STRING_TO_TIME);
                binding.setVariable("time", STRING_TO_TIME);
                if(line.equalsIgnoreCase("exit")) {
                    System.exit(0);
                }
                else if(line.equalsIgnoreCase("help")) {
                    Process p = Runtime.getRuntime().exec(
                            new String[] {
                                    "sh",
                                    "-c",
                                    "echo \"" + HELP_TEXT
                                            + "\" | less > /dev/tty" });
                    p.waitFor();
                }
                else {
                    watch.reset().start();
                    Object value = null;
                    try {
                        value = shell.evaluate(line, "ConcourseShell");
                        watch.stop();
                        if(value != null) {
                            System.out.println("Returned '" + value + "' in "
                                    + watch.elapsed(TimeUnit.MILLISECONDS)
                                    + " ms");
                        }
                    }
                    catch (Exception e) {
                        if(e.getCause() instanceof TTransportException) {
                            die(e.getMessage());
                        }
                        else {
                            System.err.print("ERROR: " + e.getMessage());
                        }
                    }

                }
                System.out.print("\n");
            }
        }
        catch (Exception e) {
            if(e.getCause() instanceof TTransportException) {
                die("Unable to connect to " + opts.username + "@" + opts.host
                        + ":" + opts.port + " with the specified password");
            }
            else {
                die(e.getMessage());
            }
        }
        finally {
            try {
                TerminalFactory.get().restore();
            }
            catch (Exception e) {
                die(e.getMessage());
            }
        }

    }

    /**
     * Print {@code message} to stderr and exit with a non-zero status.
     * 
     * @param message
     */
    private static void die(String message) {
        System.err.println("ERROR: " + message);
        System.exit(127);
    }

    /**
     * The options that can be passed to the main method of this script.
     * 
     * @author jnelson
     */
    private static class Options {

        @Parameter(names = { "-h", "--host" }, description = "The hostname where the Concourse server is located")
        public String host = "localhost";

        @Parameter(names = { "-p", "--port" }, description = "The port on which the Concourse server is listening")
        public int port = 1717;

        @Parameter(names = { "-u", "--username" }, description = "The username with which to connect")
        public String username = "admin";

        @Parameter(names = "--password", description = "The password", password = true, hidden = true)
        public String password;

        @Parameter(names = "--help", help = true, hidden = true)
        public boolean help;

    }

}
