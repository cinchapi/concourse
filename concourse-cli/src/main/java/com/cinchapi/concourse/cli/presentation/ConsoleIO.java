/*
 * Copyright (c) 2013-2019 Cinchapi Inc.
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
package com.cinchapi.concourse.cli.presentation;

import java.io.IOException;

import jline.console.ConsoleReader;

public class ConsoleIO implements IO {
    /*
     * TODO: Make this private if/when CommandLineInterface is able to change
     * it's protected API
     */
    public final ConsoleReader reader = new ConsoleReader();

    public ConsoleIO() throws IOException {}

    @Override
    public String readLine(String output, Character mask) {
        try {
            return reader.readLine(output);
        }
        catch (IOException e) {
            // TODO: possibly use `die` here
            System.err.println("ERROR: " + e.getMessage());
            System.exit(2);
            return null;
        }
    }

    @Override
    public void setExpandEvents(boolean expand) {
        reader.setExpandEvents(expand);
    }
}
