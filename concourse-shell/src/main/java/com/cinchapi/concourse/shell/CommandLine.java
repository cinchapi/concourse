/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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
package com.cinchapi.concourse.shell;

/**
 * Console related utilities.
 * 
 * @author Jeff Nelson
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
                "Copyright (c) 2013-2017, Cinchapi Inc. All Rights Reserved.")
                .append(System.lineSeparator());
        System.out.print(banner);
    }

    private CommandLine() {} /* non-initializable */

}
