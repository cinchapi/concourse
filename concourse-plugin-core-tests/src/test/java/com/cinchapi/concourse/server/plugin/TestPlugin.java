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
package com.cinchapi.concourse.server.plugin;

import ch.qos.logback.classic.Level;

/**
 * A plugin to be used in unit tests.
 * 
 * @author Jeff Nelson
 */
public class TestPlugin extends Plugin {

    /**
     * Construct a new instance.
     * @param fromServer
     * @param fromPlugin
     */
    public TestPlugin(String fromServer, String fromPlugin) {
        super(fromServer, fromPlugin);
    }
    
    public String echo(String input){
        return input;
    }

    @Override
    protected PluginConfiguration getConfig() {
        return new StandardPluginConfiguration(){

            @Override
            public Level getLogLevel() { 
                return Level.DEBUG;
            }
            
        };
    }
    
    

}
