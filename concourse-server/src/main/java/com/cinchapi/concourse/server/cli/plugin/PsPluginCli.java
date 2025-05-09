/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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
package com.cinchapi.concourse.server.cli.plugin;

import java.util.Map;

import org.apache.thrift.TException;

import com.cinchapi.concourse.server.management.ConcourseManagementService.Client;
import com.cinchapi.concourse.util.PrettyLinkedTableMap;
import com.cinchapi.lib.cli.CommandLineInterfaceInformation;

/**
 * A CLI for listing information about running plugins.
 * 
 * @author Jeff Nelson
 */
@CommandLineInterfaceInformation(description = "List information about running plugins")
class PsPluginCli extends PluginCli {

    /**
     * Construct a new instance.
     * 
     * @param options
     * @param args
     */
    public PsPluginCli(String[] args) {
        super(args);
    }

    @Override
    protected void doTask(Client client) {
        try {
            Map<Long, Map<String, String>> info = client
                    .runningPluginsInfo(token);
            info = PrettyLinkedTableMap.of(info, "pid");
            System.out.println(info);
        }
        catch (TException e) {
            halt(e.getMessage(), e);
        }

    }

}
