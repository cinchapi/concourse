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
package com.cinchapi.concourse.server.plugin;

public class ClassifiedPlugin extends TestPlugin implements Classified {

    public ClassifiedPlugin(String fromServer, String fromPlugin) {
        super(fromServer, fromPlugin);
    }

    public String open() {
        return "open";
    }
    
    @PluginRestricted
    public String secret() {
        return "secret";
    }
}
