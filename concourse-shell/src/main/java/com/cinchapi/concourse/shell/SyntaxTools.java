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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import com.google.common.base.CaseFormat;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * A collection of tools for dealing with syntax in the {@link ConcourseShell}.
 * 
 * @author Jeff Nelson
 */
public final class SyntaxTools {

    /**
     * Check {@code line} to see if it is a function call that is missing any
     * commas among arguments.
     * 
     * @param line
     * @param methods
     * @return the line with appropriate argument commas
     */
    public static String handleMissingArgCommas(String line,
            List<String> methods) {
        int hashCode = methods.hashCode();
        Set<String> hashedMethods = CACHED_METHODS.get(hashCode);
        if(hashedMethods == null) {
            hashedMethods = Sets.newHashSetWithExpectedSize(methods.size());
            hashedMethods.addAll(methods);
            CACHED_METHODS.put(hashCode, hashedMethods);
        }
        char[] chars = line.toCharArray();
        StringBuilder transformed = new StringBuilder();
        StringBuilder gather = new StringBuilder();
        boolean foundMethod = false;
        boolean inSingleQuotes = false;
        boolean inDoubleQuotes = false;
        int parenCount = 0;
        for (char c : chars) {
            if(Character.isWhitespace(c) && !foundMethod) {
                transformed.append(gather);
                transformed.append(c);
                foundMethod = hashedMethods.contains(gather.toString());
                gather.setLength(0);
            }
            else if(Character.isWhitespace(c) && foundMethod) {
                if(transformed.charAt(transformed.length() - 1) != ','
                        && !inSingleQuotes && !inDoubleQuotes && c != '\n') {
                    transformed.append(",");
                }
                transformed.append(c);
            }
            else if(c == '(' && !foundMethod) {
                parenCount++;
                transformed.append(gather);
                transformed.append(c);
                foundMethod = hashedMethods.contains(gather.toString());
                gather.setLength(0);
            }
            else if(c == '(' && foundMethod) {
                parenCount++;
                transformed.append(c);
            }
            else if(c == ';') {
                transformed.append(c);
                foundMethod = false;
                parenCount = 0;
            }
            else if(c == ')') {
                parenCount--;
                transformed.append(c);
                foundMethod = parenCount == 0 ? false : foundMethod;
            }
            else if(c == '"') {
                transformed.append(c);
                inSingleQuotes = !inSingleQuotes;
            }
            else if(c == '\'') {
                transformed.append(c);
                inDoubleQuotes = !inDoubleQuotes;
            }
            else if(foundMethod) {
                transformed.append(c);
            }
            else {
                gather.append(c);
            }
        }
        transformed.append(gather);
        return transformed.toString();
    }

    /**
     * Check to see if {@code line} is a command that uses short syntax. Short
     * syntax allows the user to call an API method without starting the command
     * with {@code concourse.}. This method compares the line to the list of
     * {@code options} to see if it should be "expanded" from short syntax.
     * Otherwise, the original line is returned.
     * 
     * @param line
     * @param options
     * @return the expanded line, if it is using short syntax, otherwise the
     *         original line
     */
    public static String handleShortSyntax(String line, List<String> options) {
        final String prepend = "concourse.";
        if(line.equalsIgnoreCase("time") || line.equalsIgnoreCase("date")) {
            return line + " \"now\"";
        }
        else if(!line.contains("(")) {
            // If there are no parens in the line, then we assume that this is a
            // single(e.g non-nested) function invocation.
            if(line.startsWith(prepend)) {
                boolean hasArgs = line.split("\\s+").length > 1;
                if(!hasArgs) {
                    line += "()";
                }
                return line;
            }
            else {
                String[] query = line.split("\\s+");
                String cmd = query[0];
                if(cmd.contains("_")) { // CON-457,GH-182
                    String replacement = CaseFormat.LOWER_UNDERSCORE.to(
                            CaseFormat.LOWER_CAMEL, cmd);
                    line = line.replaceFirst(cmd, replacement);
                }
                String expanded = prepend + line.trim();
                Pattern pattern = Pattern.compile(expanded.split("\\s|\\(")[0]);
                for (String option : options) {
                    if(pattern.matcher(option).matches()) {
                        boolean hasArgs = expanded.split("\\s+").length > 1;
                        if(!hasArgs) {
                            expanded += "()";
                        }
                        return expanded;
                    }
                }
            }
        }
        else {
            Set<String> shortInvokedMethods = parseShortInvokedMethods(line);
            for (String method : shortInvokedMethods) {
                if(options.contains(prepend + method)) {
                    line = line.replaceAll("(?<!\\_)" + method + "\\(", prepend
                            + method + "\\(");
                }
            }
        }
        return line;
    }

    /**
     * Examine a line and parse out the names of all the methods that are being
     * invoked using short syntax.
     * <p>
     * e.g. methodA(methodB(x), time(y), methodC(concourse.methodD())) -->
     * methodA, methodB, methodC
     * </p>
     * 
     * @param line
     * @return the set of all the methods which are being invoked using short
     *         syntax
     */
    protected static Set<String> parseShortInvokedMethods(String line) { // visible
                                                                         // for
                                                                         // testing
        Set<String> methods = Sets.newHashSet();
        Set<String> blacklist = Sets.newHashSet("time", "date");
        String regex = "\\b(?!" + StringUtils.join(blacklist, "|")
                + ")[\\w\\.]+\\("; // match any word followed by an paren except
                                   // for the blacklist
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(line);
        while (matcher.find()) {
            if(!matcher.group().startsWith("concourse.")) {
                methods.add(matcher.group().replace("(", ""));
            }
        }
        return methods;
    }

    /**
     * For the methods in this class that take a list of callable methods, this
     * collection will map the hashcode of that list to a hashset with the same
     * methods. The hashset can be used for more efficient O(1) lookups as
     * opposed to always iterating through the list.
     */
    private static Map<Integer, Set<String>> CACHED_METHODS = Maps
            .newHashMapWithExpectedSize(1);

    private SyntaxTools() {/* noop */}

}
