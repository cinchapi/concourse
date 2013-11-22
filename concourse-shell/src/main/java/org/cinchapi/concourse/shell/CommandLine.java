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

/**
 * Console related utilities.
 * 
 * @author jnelson
 */
public final class CommandLine {

    /**
     * Display a welcome banner to System.out
     */
    public static void displayWelcomeBanner() {
        StringBuilder banner = new StringBuilder();
        banner.append(" _____").append(System.lineSeparator());
        banner.append("/  __ \\").append(System.lineSeparator());
        banner.append("| /  \\/ ___  _ __   ___ ___  _   _ _ __ ___  ___")
                .append(System.lineSeparator());
        banner.append("| |    / _ \\| '_ \\ / __/ _ \\| | | | '__/ __|/ _ \\")
                .append(System.lineSeparator());
        banner.append("| \\__/\\ (_) | | | | (_| (_) | |_| | |  \\__ \\  __/")
                .append(System.lineSeparator());
        banner.append(" \\____/\\___/|_| |_|\\___\\___/ \\__,_|_|  |___/\\___|")
                .append(System.lineSeparator());
        banner.append("").append(System.lineSeparator());
        banner.append(
                "Copyright (c) 2013, Cinchapi Software Collective, LLC. All Rights Reserved.")
                .append(System.lineSeparator());
        System.out.print(banner);
    }

    private CommandLine() {} /* non-initializable */

}
