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
package org.cinchapi.concourse.shell;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.text.MessageFormat;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import groovy.lang.Binding;
import groovy.lang.Closure;
import groovy.lang.GroovyShell;

import jline.TerminalFactory;
import jline.console.ConsoleReader;
import jline.console.completer.StringsCompleter;

import org.apache.thrift.TApplicationException;
import org.apache.thrift.transport.TTransportException;
import org.cinchapi.concourse.Concourse;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TSecurityException;
import org.cinchapi.concourse.Timestamp;
import org.cinchapi.concourse.util.Version;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.clutch.dates.StringToTime;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
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
            Concourse concourse = Concourse.connect(opts.host, opts.port,
                    opts.username, opts.password);

            CommandLine.displayWelcomeBanner();
            Binding binding = new Binding();
            GroovyShell shell = new GroovyShell(binding);

            Stopwatch watch = Stopwatch.createUnstarted();
            console.println("Client Version "
                    + Version.getVersion(ConcourseShell.class));
            console.println("Server Version " + concourse.getServerVersion());
            console.println("");
            console.println("Type HELP for help.");
            console.println("Type EXIT to quit.");
            console.println("Use TAB for completion.");
            console.setPrompt("cash$ ");
            console.addCompleter(new StringsCompleter(
                    getAccessibleApiMethodsUsingShortSyntax()));

            final List<String> methods = Lists
                    .newArrayList(getAccessibleApiMethods());
            String line;
            while ((line = console.readLine().trim()) != null) {
                line = SyntaxTools.handleShortSyntax(line, methods);
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
                    concourse.exit();
                    System.exit(0);
                }
                else if(line.equalsIgnoreCase("help")
                        || line.equalsIgnoreCase("man")) {
                    Process p = Runtime.getRuntime().exec(
                            new String[] {
                                    "sh",
                                    "-c",
                                    "echo \"" + HELP_TEXT
                                            + "\" | less > /dev/tty" });
                    p.waitFor();
                }
                else if(containsBannedCharSequence(line)) {
                    System.err.println("Cannot complete command because "
                            + "it contains an illegal character sequence.");
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
                        else {
                            System.out.println("Completed in "
                                    + watch.elapsed(TimeUnit.MILLISECONDS)
                                    + " ms");
                        }
                    }
                    catch (Exception e) {
                        if(e.getCause() instanceof TTransportException) {
                            die(e.getMessage());
                        }
                        else if(e.getCause() instanceof TSecurityException) {
                            die("A security change has occurred and your session cannot continue");
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
            else if(e.getCause() instanceof TApplicationException
                    && e.getMessage().contains(
                            "Internal error processing login")) {
                die("Invalid username/password combination.");
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
     * Return a sorted array that contains all the accessible API methods.
     * 
     * @return the accessible API methods
     */
    protected static String[] getAccessibleApiMethods() {
        if(ACCESSIBLE_API_METHODS == null) {
            Set<String> banned = Sets.newHashSet("equals", "getClass",
                    "hashCode", "notify", "notifyAll", "toString", "wait",
                    "exit");
            Set<String> methods = Sets.newTreeSet();
            for (Method method : Concourse.class.getMethods()) {
                if(!Modifier.isStatic(method.getModifiers())
                        && !banned.contains(method.getName())) {
                    // NOTE: Even though the StringCompleter strips the
                    // "concourse." from these method names, we must add it here
                    // so that we can properly handle short syntax in
                    // SyntaxTools#handleShortSyntax
                    methods.add(MessageFormat.format("concourse.{0}",
                            method.getName()));
                }
            }
            ACCESSIBLE_API_METHODS = methods
                    .toArray(new String[methods.size()]);
        }
        return ACCESSIBLE_API_METHODS;

    }

    /**
     * Return {@code true} if {@code string} contains at last one of the
     * {@link #BANNED_CHAR_SEQUENCES} strings.
     * 
     * @param string
     * @return {@code true} if string contains a banned character sequence
     */
    private static boolean containsBannedCharSequence(String string) {
        for (String charSequence : BANNED_CHAR_SEQUENCES) {
            if(string.contains(charSequence)) {
                return true;
            }
        }
        return false;
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
     * Return a sorted array that contains all the accessible API methods using
     * short syntax.
     * 
     * @return the accessible API methods using short syntax
     */
    private static String[] getAccessibleApiMethodsUsingShortSyntax() {
        Set<String> methods = Sets.newTreeSet();
        for (String method : getAccessibleApiMethods()) {
            methods.add(method.replace("concourse.", ""));
        }
        return methods.toArray(new String[methods.size()]);
    }

    /**
     * A list of char sequences that we must ban for security and other
     * miscellaneous purposes.
     */
    private static List<String> BANNED_CHAR_SEQUENCES = Lists.newArrayList(
            "concourse.exit()", "concourse.username", "concourse.password",
            "concourse.client");

    /**
     * A cache of the API methods that are accessible in CaSH.
     */
    private static String[] ACCESSIBLE_API_METHODS = null;

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
     * A closure that converts a string description to a timestamp.
     */
    private static Closure<Timestamp> STRING_TO_TIME = new Closure<Timestamp>(
            null) {

        private static final long serialVersionUID = 1L;

        @Override
        public Timestamp call(Object arg) {
            if(Longs.tryParse(arg.toString()) != null) {
                // We should assume that the timestamp is in microseconds since
                // that is the output format used in ConcourseShell
                return Timestamp.fromMicros(Long.parseLong(arg.toString()));
            }
            else {
                return Timestamp.fromJoda(StringToTime.parseDateTime(arg
                        .toString()));
            }
        }

    };

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

        @Parameter(names = "--password", description = "The password", password = false, hidden = true)
        public String password;

        @Parameter(names = "--help", help = true, hidden = true)
        public boolean help;

    }

}
