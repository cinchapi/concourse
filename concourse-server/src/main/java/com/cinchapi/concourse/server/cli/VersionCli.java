/*
 * Copyright (c) 2013-2016 Cinchapi Inc.
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
package com.cinchapi.concourse.server.cli;

import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.shell.CommandLine;
import com.cinchapi.concourse.util.Version;

/**
 * A simple CLI that displays information about the Concourse version.
 * 
 * @author Jeff Nelson
 */
public final class VersionCli {
    
    /**
     * Run the program...
     * @param args
     */
    public static void main(String...args){
        CommandLine.displayWelcomeBanner();
        System.out.println("Concourse Server "+Version.getVersion(VersionCli.class));
        System.out.println("System Id: "+GlobalState.SYSTEM_ID);
    }

}
