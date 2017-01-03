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
 * Parent interface for all plugin hooks.
 * 
 * @author Jeff Nelson
 */
public interface PluginHook {

    /**
     * Given the installation {@link PluginContext context} for a plugin,
     * execute a hook to configure its runtime behavior.
     * <p>
     * The hook is assumed to have run successfully if this method terminates
     * without exception. Whenever an exception is thrown by a hook, the parent
     * process that invokes the hook (i.e installation or starting the plugin,
     * etc) will fail and Concourse Server will communicate the failure to the
     * user.
     * </p>
     * 
     * @param context the plugin's {@link PluginContext context}
     */
    public void run(PluginContext context);

}
