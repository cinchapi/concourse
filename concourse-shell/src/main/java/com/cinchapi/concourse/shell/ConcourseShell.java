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
package com.cinchapi.concourse.shell;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.text.MessageFormat.format;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import groovy.lang.Binding;
import groovy.lang.Closure;
import groovy.lang.GroovyShell;
import groovy.lang.MissingMethodException;
import groovy.lang.Script;
import jline.TerminalFactory;
import jline.console.ConsoleReader;
import jline.console.UserInterruptException;
import jline.console.completer.StringsCompleter;
import jline.console.history.FileHistory;

import org.apache.thrift.transport.TTransportException;

import com.cinchapi.concourse.config.ConcourseClientPreferences;

import org.codehaus.groovy.control.CompilationFailedException;
import org.codehaus.groovy.control.MultipleCompilationErrorsException;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.Link;
import com.cinchapi.concourse.Tag;
import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.StartState;
import com.cinchapi.concourse.thrift.Diff;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.thrift.ParseException;
import com.cinchapi.concourse.thrift.SecurityException;
import com.cinchapi.concourse.util.FileOps;
import com.cinchapi.concourse.util.Version;
import com.google.common.base.CaseFormat;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * The main program runner for the ConcourseShell client. ConcourseShell wraps a
 * connection to a ConcourseServer inside of a {@link GroovyShell}, which allows
 * for rich interaction with Concourse in a scripting environment.
 * 
 * @author Jeff Nelson
 */
public final class ConcourseShell {

    /**
     * Run the program...
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String... args) throws Exception {
        try {
            ConcourseShell cash = new ConcourseShell();
            Options opts = new Options();
            JCommander parser = null;
            try {
                parser = new JCommander(opts, args);
            }
            catch (Exception e) {
                die(e.getMessage());
            }
            parser.setProgramName("concourse-shell");
            if(opts.help) {
                parser.usage();
                System.exit(1);
            }
            if(!Strings.isNullOrEmpty(opts.prefs)) {
                opts.prefs = FileOps.expandPath(opts.prefs,
                        System.getProperty("user.dir.real"));
                ConcourseClientPreferences prefs = ConcourseClientPreferences
                        .open(opts.prefs);
                opts.username = prefs.getUsername();
                opts.password = new String(prefs.getPasswordExplicit());
                opts.host = prefs.getHost();
                opts.port = prefs.getPort();
                opts.environment = prefs.getEnvironment();
            }
            if(Strings.isNullOrEmpty(opts.password)) {
                cash.setExpandEvents(false);
                opts.password = cash.console.readLine("Password ["
                        + opts.username + "]: ", '*');
            }
            try {
                cash.concourse = Concourse.connect(opts.host, opts.port,
                        opts.username, opts.password, opts.environment);
                cash.whoami = opts.username;
            }
            catch (Exception e) {
                if(e.getCause() instanceof TTransportException) {
                    die("Unable to connect to the Concourse Server at "
                            + opts.host + ":" + opts.port);
                }
                else if(e.getCause() instanceof SecurityException) {
                    die("Invalid username/password combination.");
                }
                else {
                    die(e.getMessage());
                }
            }
            if(!opts.ignoreRunCommands) {
                cash.loadExternalScript(opts.ext);
            }
            if(!Strings.isNullOrEmpty(opts.run)) {
                try {
                    String result = cash.evaluate(opts.run);
                    System.out.println(result);
                    cash.concourse.exit();
                    System.exit(0);
                }
                catch (IrregularEvaluationResult e) {
                    die(e.getMessage());
                }
            }
            else {
                cash.enableInteractiveSettings();
                boolean running = true;
                String input = "";
                while (running) {
                    boolean extraLineBreak = true;
                    boolean clearInput = true;
                    boolean clearPrompt = false;
                    try {
                        input = input + cash.console.readLine().trim();
                        String result = cash.evaluate(input);
                        System.out.println(result);
                    }
                    catch (UserInterruptException e) {
                        if(Strings.isNullOrEmpty(e.getPartialLine())
                                && Strings.isNullOrEmpty(input)) {
                            cash.console.println("Type EXIT to quit.");
                        }
                    }
                    catch (HelpRequest e) {
                        String text = getHelpText(e.topic);
                        if(!Strings.isNullOrEmpty(text)) {
                            Process p = Runtime.getRuntime().exec(
                                    new String[] {
                                            "sh",
                                            "-c",
                                            "echo \"" + text
                                                    + "\" | less > /dev/tty" });
                            p.waitFor();
                        }
                        cash.console.getHistory().removeLast();
                    }
                    catch (ExitRequest e) {
                        running = false;
                        cash.console.getHistory().removeLast();
                    }
                    catch (NewLineRequest e) {
                        extraLineBreak = false;
                    }
                    catch (ProgramCrash e) {
                        die(e.getMessage());
                    }
                    catch (MultiLineRequest e) {
                        extraLineBreak = false;
                        clearInput = false;
                        clearPrompt = true;
                    }
                    catch (IrregularEvaluationResult e) {
                        System.err.println(e.getMessage());
                    }
                    finally {
                        if(extraLineBreak) {
                            cash.console.print("\n");
                        }
                        if(clearInput) {
                            input = "";
                        }
                        if(clearPrompt) {
                            cash.console.setPrompt("> ");
                        }
                        else {
                            cash.console.setPrompt(cash.defaultPrompt);
                        }
                    }
                }
                cash.concourse.exit();
                System.exit(0);
            }
        }
        finally {
            TerminalFactory.get().restore();
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
                    methods.add(format("concourse.{0}", method.getName()));
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
        System.exit(1);
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
        // Add the Showable items
        for (Showable showable : Showable.values()) {
            methods.add("show " + showable.getName());
        }
        return methods.toArray(new String[methods.size()]);
    }

    /**
     * Return the help text for a given {@code topic}.
     * 
     * @param topic
     * @return the help text
     */
    private static String getHelpText(String topic) {
        topic = Strings.isNullOrEmpty(topic) ? "cash" : topic;
        topic = topic.toLowerCase();
        InputStream in = ConcourseShell.class.getResourceAsStream("/" + topic);
        if(in == null) {
            System.err.println("No help entry for " + topic);
            return null;
        }
        else {
            try {
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(in));
                StringBuilder builder = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    line = line.replaceAll("\"", "\\\\\"");
                    builder.append(line).append(
                            System.getProperty("line.separator"));
                }
                String text = builder.toString().trim();
                reader.close();
                return text;
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    /**
     * Attempt to return the {@link #ACCESSIBLE_API_METHODS API method} that is
     * the closest match for the specified {@code alias}.
     * <p>
     * This method can be used to take a user supplied method name that does not
     * match any of the {@link #ACCESSIBLE_API_METHODS provided} ones, but can
     * be reasonably assumed to be a valid alias of some sort (i.e. an API
     * method name in underscore case as opposed to camel case).
     * </p>
     * 
     * @param alias the method name that may be an alias for one of the provided
     *            API methods
     * @return the actual API method that {@code alias} should resolve to, if it
     *         is possible to determine that; otherwise {@code null}
     */
    @Nullable
    private static String tryGetCorrectApiMethod(String alias) {
        String camel = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,
                alias);
        String expanded = com.cinchapi.concourse.util.Strings.ensureStartsWith(
                camel, "concourse.");
        return methods.contains(expanded) && !camel.equals(alias) ? camel
                : null;
    }

    /**
     * A cache of the API methods that are accessible in CaSH.
     */
    private static String[] ACCESSIBLE_API_METHODS = null;

    /**
     * A list of char sequences that we must ban for security and other
     * miscellaneous purposes.
     */
    private static List<String> BANNED_CHAR_SEQUENCES = ImmutableList.of(
            "concourse.exit()", "concourse.username", "concourse.password",
            "concourse.client", "concourse.getClass().getDeclaredFields()",
            Concourse.class.getName());

    /**
     * The message to display when a line of input contains a banned character
     * sequence.
     */
    protected static final String BANNED_CHAR_SEQUENCE_ERROR_MESSAGE = "Cannot evaluate input "
            + "because it contains an illegal character sequence"; // visible
                                                                   // for
                                                                   // testing

    /**
     * A list which contains all of the accessible API methods. This list is
     * used to expand short syntax that is used in any evaluatable line.
     */
    private static final List<String> methods = Lists
            .newArrayList(getAccessibleApiMethods());

    /**
     * The name of the external script that is
     * {@link #loadExternalScript(String)
     * loaded}. This name is how the script functions are stored in the
     * {@link #groovyBinding}.
     */
    private static final String EXTERNAL_SCRIPT_NAME = "ext";

    /**
     * The list of classes that are imported directly into the
     * {@link #groovyBinding} so that they can be used within CaSH the exact
     * same way they would in code. Importing classes directly eliminates the
     * need to bind custom closures to static methods in the classes.
     */
    protected static List<Class<?>> IMPORTED_CLASSES = ImmutableList.of(
            Timestamp.class, Diff.class, Link.class, Tag.class, Criteria.class,
            Operator.class); // visible for testing

    /**
     * A closure that converts a string value to a tag.
     * 
     * @deprecated Use the {@link Tag} class directly as it is imported into the
     *             {@link #groovyBinding} within the {@link #evaluate(String)}
     *             method.
     */
    @Deprecated
    private static Closure<Tag> STRING_TO_TAG = new Closure<Tag>(null) {

        private static final long serialVersionUID = 1L;

        @Override
        public Tag call(Object arg) {
            return Tag.create(arg.toString());
        }

    };

    /**
     * A closure that returns a nwe CriteriaBuilder object.
     * 
     * @deprecated Use the {@link Criteria} class directly as it is imported
     *             into the {@link #groovyBinding} within the
     *             {@link #evaluate(String)} method.
     */
    @Deprecated
    private static Closure<StartState> WHERE = new Closure<StartState>(null) {

        private static final long serialVersionUID = 1L;

        @Override
        public StartState call() {
            return Criteria.where();
        }

    };

    /**
     * The client connection to Concourse.
     */
    protected Concourse concourse;

    /**
     * The file where the user's CASH history is stored.
     */
    protected String historyStore = System.getProperty("user.home")
            + File.separator + ".cash_history";

    /**
     * The Concourse user that is currently connected to the shell.
     */
    protected String whoami;

    /**
     * The env that the client is connected to.
     */
    protected String env;

    /**
     * The console handles all I/O.
     */
    private ConsoleReader console;

    /**
     * The groovy environment that actually evaluates the user input.
     */
    private GroovyShell groovy;

    /**
     * The binding that contains all the variables that are in scope for the
     * groovy environment.
     */
    private Binding groovyBinding;

    /**
     * The stopwatch that is used to time the duration of all evaluations.
     */
    private Stopwatch watch = Stopwatch.createUnstarted();

    /**
     * The shell prompt.
     */
    private String defaultPrompt;

    /**
     * An external script that has been loaded by the
     * {@link #loadExternalScript(String)} method.
     */
    private Script script = null;

    /**
     * A closure that responds to the 'show' command and returns information to
     * display to the user based on the input argument(s).
     */
    private final Closure<Object> showFunction = new Closure<Object>(null) {

        private static final long serialVersionUID = 1L;

        @Override
        public Object call(Object arg) {
            if(arg == Showable.RECORDS) {
                return concourse.inventory();
            }
            else {
                StringBuilder sb = new StringBuilder();
                sb.append("Unable to show ");
                sb.append(arg);
                sb.append(". Valid options are: ");
                sb.append(Showable.OPTIONS);
                throw new IllegalArgumentException(sb.toString());
            }
        }

    };

    /**
     * A closure that responds to time/date alias functions.
     */
    private final Closure<Timestamp> timeFunction = new Closure<Timestamp>(null) {

        private static final long serialVersionUID = 1L;

        @Override
        public Timestamp call(Object arg) {
            return concourse.time(arg.toString());
        }

        @Override
        public Timestamp call() {
            return concourse.time();
        }
    };

    /**
     * Construct a new instance. Be sure to call {@link #setClient(Concourse)}
     * before performing any
     * evaluations.
     * 
     * @throws Exception
     */
    protected ConcourseShell() throws Exception {
        this.console = new ConsoleReader();
        this.groovyBinding = new Binding();
        this.groovy = new GroovyShell(groovyBinding);
    }

    /**
     * Evaluate the given {@code input} and return the result that should be
     * displayed to the user. If, for some reason, the evaluation of the input
     * does not yield a displayable response, a subclass of
     * {@link IrregularEvaluationResult} will be thrown to give the caller and
     * indication of how to proceed.
     * 
     * @param input
     * @return the result of the evaluation
     * @throws IrregularEvaluationResult
     */
    public String evaluate(String input) throws IrregularEvaluationResult {
        input = SyntaxTools.handleShortSyntax(input, methods);
        String inputLowerCase = input.toLowerCase();

        // NOTE: These must always be set before evaluating a line just in case
        // an attempt was made to bind the variables to different values in a
        // previous evaluation.
        groovyBinding.setVariable("concourse", concourse);
        groovyBinding.setVariable("eq", Operator.EQUALS);
        groovyBinding.setVariable("ne", Operator.NOT_EQUALS);
        groovyBinding.setVariable("gt", Operator.GREATER_THAN);
        groovyBinding.setVariable("gte", Operator.GREATER_THAN_OR_EQUALS);
        groovyBinding.setVariable("lt", Operator.LESS_THAN);
        groovyBinding.setVariable("lte", Operator.LESS_THAN_OR_EQUALS);
        groovyBinding.setVariable("bw", Operator.BETWEEN);
        groovyBinding.setVariable("regex", Operator.REGEX);
        groovyBinding.setVariable("nregex", Operator.NOT_REGEX);
        groovyBinding.setVariable("lnk2", Operator.LINKS_TO);
        groovyBinding.setVariable("time", timeFunction);
        groovyBinding.setVariable("date", timeFunction);
        groovyBinding.setVariable("where", WHERE); // deprecated
        groovyBinding.setVariable("tag", STRING_TO_TAG); // deprecated
        groovyBinding.setVariable("whoami", whoami);
        groovyBinding.setVariable("ADDED", Diff.ADDED);
        groovyBinding.setVariable("REMOVED", Diff.REMOVED);
        // Do direct import of declared classes
        for (Class<?> clazz : IMPORTED_CLASSES) {
            String variable = clazz.getSimpleName();
            groovyBinding.setVariable(variable, clazz);
        }
        // Add Showable variables
        for (Showable showable : Showable.values()) {
            groovyBinding.setVariable(showable.getName(), showable);
        }
        groovyBinding.setVariable("show", showFunction);
        if(script != null) {
            groovyBinding.setVariable(EXTERNAL_SCRIPT_NAME, script);
        }
        if(inputLowerCase.equalsIgnoreCase("exit")) {
            throw new ExitRequest();
        }
        else if(inputLowerCase.startsWith("help")
                || inputLowerCase.startsWith("man")) {
            String[] toks = input.split(" ");
            if(toks.length == 1) {
                throw new HelpRequest();
            }
            else {
                String topic = toks[1];
                throw new HelpRequest(topic);
            }
        }
        else if(containsBannedCharSequence(input)) {
            throw new EvaluationException(BANNED_CHAR_SEQUENCE_ERROR_MESSAGE);
        }
        else if(Strings.isNullOrEmpty(input)) { // CON-170
            throw new NewLineRequest();
        }
        else {
            StringBuilder result = new StringBuilder();
            try {
                watch.reset().start();
                Object value = groovy.evaluate(input, "ConcourseShell");
                watch.stop();
                long elapsed = watch.elapsed(TimeUnit.MILLISECONDS);
                double seconds = elapsed / 1000.0;
                if(value != null) {
                    result.append("Returned '" + value + "' in " + seconds
                            + " sec");
                }
                else {
                    result.append("Completed in " + seconds + " sec");
                }
                return result.toString();
            }
            catch (CompilationFailedException e) {
                throw new MultiLineRequest(e.getMessage());
            }
            catch (Exception e) {
                // CON-331: Here we catch a generic Exception and examine
                // additional context (i.e. the cause or other environmental
                // aspects) to perform additional logic that determines the
                // appropriate response. These cases SHOULD NOT be placed in
                // their own separate catch block.
                String method = null;
                String methodCorrected = null;
                if(e.getCause() instanceof TTransportException) {
                    throw new ProgramCrash(e.getMessage());
                }
                else if(e.getCause() instanceof SecurityException) {
                    throw new ProgramCrash(
                            "A security change has occurred and your "
                                    + "session cannot continue");
                }
                else if(e instanceof MissingMethodException
                        && ErrorCause.determine(e.getMessage()) == ErrorCause.MISSING_CASH_METHOD
                        && ((methodCorrected = tryGetCorrectApiMethod((method = ((MissingMethodException) e)
                                .getMethod()))) != null || hasExternalScript())) {
                    if(methodCorrected != null) {
                        input = input.replaceAll(method, methodCorrected);
                    }
                    else {
                        input = input.replaceAll(method, "ext." + method);
                    }
                    return evaluate(input);
                }
                else {
                    String message = e.getCause() instanceof ParseException ? e
                            .getCause().getMessage() : e.getMessage();
                    throw new EvaluationException("ERROR: " + message);
                }
            }
        }
    }

    /**
     * Return {@code true} if this instance has an external script loaded.
     * 
     * @return {@code true} if an external script has been loaded
     */
    public boolean hasExternalScript() {
        return script != null;
    }

    /**
     * Load an external script and store it so it can be added to the binding
     * when {@link #evaluate(String) evaluating} commands. Any functions defined
     * in the script must be accessed using the {@code ext} qualifier.
     * 
     * @param script - the path to the external script
     */
    public void loadExternalScript(String script) {
        try {
            Path extPath = Paths.get(script);
            if(Files.exists(extPath) && Files.size(extPath) > 0) {
                List<String> lines = FileOps.readLines(script);
                StringBuilder sb = new StringBuilder();
                for (String line : lines) {
                    line = SyntaxTools.handleShortSyntax(line, methods);
                    sb.append(line)
                            .append(System.getProperty("line.separator"));
                }
                String scriptText = sb.toString();
                try {
                    this.script = groovy
                            .parse(scriptText, EXTERNAL_SCRIPT_NAME);
                    evaluate(scriptText);
                }
                catch (IrregularEvaluationResult e) {
                    System.err.println(e.getMessage());
                }
                catch (MultipleCompilationErrorsException e) {
                    String msg = e.getMessage();
                    msg = msg.substring(msg.indexOf('\n') + 1);
                    msg = msg.replaceAll("ext: ", "");
                    die("A fatal error occurred while parsing the run-commands file at "
                            + script
                            + System.getProperty("line.separator")
                            + msg
                            + System.getProperty("line.separator")
                            + "Fix these errors or start concourse shell with the --no-run-commands flag");
                }
            }
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * This method calls {@link ConsoleReader#setExpandEvents(boolean)} with the
     * specified value.
     * 
     * @param bool
     */
    public void setExpandEvents(boolean bool) {
        console.setExpandEvents(bool);
    }

    /**
     * Turn on the settings necessary to make the application "interactive"
     * (e.g. a REPL). By default, these settings are turned off.
     */
    private void enableInteractiveSettings() throws Exception {
        console.setExpandEvents(false);
        console.setHandleUserInterrupt(true);
        File file = new File(historyStore);
        file.createNewFile();
        console.setHistory(new FileHistory(file));
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    ((FileHistory) console.getHistory()).flush();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }

        }));
        CommandLine.displayWelcomeBanner();
        env = concourse.getServerEnvironment();
        setDefaultPrompt();
        console.println("Client Version "
                + Version.getVersion(ConcourseShell.class));
        console.println("Server Version " + concourse.getServerVersion());
        console.println("");
        console.println("Connected to the '" + env + "' environment.");
        console.println("");
        console.println("Type HELP for help.");
        console.println("Type EXIT to quit.");
        console.println("Use TAB for completion.");
        console.println("");
        console.setPrompt(defaultPrompt);
        console.addCompleter(new StringsCompleter(
                getAccessibleApiMethodsUsingShortSyntax()));
    }

    /**
     * Set the {@link #defaultPrompt} variable to account for the current
     * {@link #env}.
     */
    private void setDefaultPrompt() {
        this.defaultPrompt = format("[{0}/cash]$ ", env);
    }

    /**
     * The options that can be passed to the main method of this script.
     * 
     * @author Jeff Nelson
     */
    private static class Options {

        /**
         * A handler for the client preferences that <em>may</em> exist in the
         * user's home directory.
         */
        private ConcourseClientPreferences prefsHandler = null;

        {
            String file = System.getProperty("user.home") + File.separator
                    + "concourse_client.prefs";
            if(Files.exists(Paths.get(file))) { // check to make sure that the
                                                // file exists first, so we
                                                // don't create a blank one if
                                                // it doesn't
                prefsHandler = ConcourseClientPreferences.open(file);
            }
        }

        @Parameter(names = { "-e", "--environment" }, description = "The environment of the Concourse Server to use")
        public String environment = prefsHandler != null ? prefsHandler
                .getEnvironment() : "";

        @Parameter(names = "--help", help = true, hidden = true)
        public boolean help;

        @Parameter(names = { "-h", "--host" }, description = "The hostname where the Concourse Server is located")
        public String host = prefsHandler != null ? prefsHandler.getHost()
                : "localhost";

        @Parameter(names = "--password", description = "The password", password = false, hidden = true)
        public String password = prefsHandler != null ? new String(
                prefsHandler.getPasswordExplicit()) : null;

        @Parameter(names = { "-p", "--port" }, description = "The port on which the Concourse Server is listening")
        public int port = prefsHandler != null ? prefsHandler.getPort() : 1717;

        @Parameter(names = { "-r", "--run" }, description = "The command to run non-interactively")
        public String run = "";

        @Parameter(names = { "-u", "--username" }, description = "The username with which to connect")
        public String username = prefsHandler != null ? prefsHandler
                .getUsername() : "admin";

        @Parameter(names = { "--run-commands", "--rc" }, description = "Path to a script that contains commands to run when the shell starts")
        public String ext = FileOps.getUserHome() + "/.cashrc";

        @Parameter(names = { "--no-run-commands", "--no-rc" }, description = "A flag to disable loading any run commands file")
        public boolean ignoreRunCommands = false;

        @Parameter(names = "--prefs", description = "Path to the concourse_client.prefs file")
        public String prefs;

    }

    /**
     * An enum that summarizes the cause of an error based on the message.
     * <p>
     * Retrieve an instance by calling {@link ErrorCause#determine(String)} on
     * an error message returned from an exception/
     * </p>
     * 
     * @author Jeff Nelson
     */
    private enum ErrorCause {
        MISSING_CASH_METHOD, MISSING_EXTERNAL_METHOD, UNDEFINED;

        /**
         * Examine an error message to determine the {@link ErrorCause}.
         * 
         * @param message - the error message from an Exception
         * @return the {@link ErrorCause} that summarizes the reason the
         *         Exception occurred
         */
        public static ErrorCause determine(String message) {
            if(message.startsWith("No signature of method: ConcourseShell.")) {
                return MISSING_CASH_METHOD;
            }
            else if(message.startsWith("No signature of method: ext.")) {
                return MISSING_EXTERNAL_METHOD;
            }
            else {
                return UNDEFINED;
            }
        }
    }

    /**
     * An enum containing the types of things that can be listed using the
     * 'show' function.
     * 
     * @author Jeff Nelson
     */
    private enum Showable {
        RECORDS;

        /**
         * Return the name of this Showable.
         * 
         * @return the name
         */
        public String getName() {
            return name().toLowerCase();
        }

        /**
         * Valid options for the 'show' function based on the values defined in
         * this enum.
         */
        private static String OPTIONS;
        static {
            StringBuilder sb = new StringBuilder();
            for (Showable showable : values()) {
                sb.append(showable.toString().toLowerCase()).append(" ");
            }
            sb.deleteCharAt(sb.length() - 1);
            OPTIONS = sb.toString();
        }

    }

}