/*
 * Copyright (c) 2013-2015 Cinchapi, Inc.
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
package org.cinchapi.concourse.cli;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.cinchapi.concourse.config.ConcourseClientPreferences;

import com.beust.jcommander.Parameter;

/**
 * Each member variable represents the options that can be passed to the main
 * method of a CLI. Each CLI should (anonymously) subclass this and specify the
 * appropriate parameters.
 * 
 * <p>
 * See http://jcommander.org/ for more information.
 * <p>
 * 
 * @author Jeff Nelson
 */
public class Options {

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
            prefsHandler = ConcourseClientPreferences.load(file);
        }
    }

    @Parameter(names = { "--help" }, help = true, hidden = true)
    public boolean help;

    @Parameter(names = { "-h", "--host" }, description = "The hostname where the Concourse Server is located")
    public String host = prefsHandler != null ? prefsHandler.getHost()
            : "localhost";

    @Parameter(names = { "-p", "--port" }, description = "The port on which the Concourse Server is listening")
    public int port = prefsHandler != null ? prefsHandler.getPort() : 1717;

    @Parameter(names = { "-u", "--username" }, description = "The username with which to connect")
    public String username = prefsHandler != null ? prefsHandler.getUsername()
            : "admin";

    @Parameter(names = "--password", description = "The password", password = false, hidden = true)
    public String password = prefsHandler != null ? new String(
            prefsHandler.getPassword()) : null;

    @Parameter(names = { "-e", "--environment" }, description = "The environment of the Concourse Server to use")
    public String environment = prefsHandler != null ? prefsHandler
            .getEnvironment() : "";

    @Parameter(names = "--prefs", description = "Path to the concourse_client.prefs file")
    public String prefs;

}
