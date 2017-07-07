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
package com.cinchapi.concourse.server.plugin.hook;

import com.cinchapi.concourse.server.plugin.PluginContext;

/**
 * A hook that is run <strong>after</strong> the plugin is installed.
 * 
 * @author Jeff Nelson
 */
@FunctionalInterface
public interface AfterInstallHook extends PluginHook{

    /**
     * Given the newly installed plugin's {@code context}, execute additional
     * pre-launch logic. If this method return {@code false} the plugin
     * installation will rollback. Otherwise, the plugin's installation will be
     * finalized and the plugin will launch.
     * 
     * @param context the plugin's {@link PluginContext context}
     * @return {@code true} if the plugin's installation can be finalized
     */
    public void run(PluginContext context);

}
