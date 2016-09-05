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
package com.cinchapi.concourse.server.plugin;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentMap;

import com.cinchapi.common.logging.Logger;
import com.cinchapi.concourse.annotate.PackagePrivate;
import com.cinchapi.concourse.server.plugin.io.SharedMemory;
import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.util.ByteBuffers;
import com.cinchapi.concourse.util.ConcurrentMaps;
import com.cinchapi.concourse.util.Serializables;
import com.google.common.collect.Maps;

/**
 * A {@link Plugin} extends the functionality of Concourse Server.
 * <p>
 * Each class that extends this one may define methods that can be dynamically
 * invoked using the
 * {@link com.cinchapi.concourse.Concourse#invokePlugin(String, String, Object...)
 * invokePlugin} method.
 * </p>
 * 
 * @author Jeff Nelson
 */
public abstract class Plugin {

    /**
     * The name of the dynamic property that is passed to the plugin's JVM to
     * instruct it as to where the plugin's home is located.
     */
    protected final static String PLUGIN_HOME_JVM_PROPERTY = "com.cinchapi.concourse.plugin.home";

    /**
     * The communication channel for messages that come from Concourse Server,
     */
    protected final SharedMemory fromServer;

    /**
     * A reference to the local Concourse Server {@link ConcourseRuntime
     * runtime} to which this plugin is registered.
     */
    protected final ConcourseRuntime runtime;

    /**
     * A {@link Logger} for plugin operations.
     */
    protected final Logger log;

    /**
     * The communication channel for messages that are sent by this
     * {@link Plugin} to Concourse Server.
     */
    private final SharedMemory fromPlugin;

    /**
     * Upstream response from Concourse Server in response to requests made via
     * {@link ConcourseRuntime}.
     */
    private final ConcurrentMap<AccessToken, RemoteMethodResponse> fromServerResponses;

    /**
     * <strong>DO NOT CALL!!!!</strong>
     * <p>
     * Internal constructor used by Concourse Server to run the
     * {@link #afterInstall()} hook.
     * </p>
     */
    @SuppressWarnings("unused")
    private Plugin() {
        this.runtime = null;
        this.fromServer = null;
        this.fromPlugin = null;
        this.fromServerResponses = null;
        this.log = null;
    }

    /**
     * Construct a new instance.
     * 
     * @param fromServer the location where Concourse Server places messages to
     *            be consumed by the Plugin
     * @param fromPlugin the location where the Plugin places messages to be
     *            consumed by Concourse Server
     */
    public Plugin(String fromServer, String fromPlugin) {
        this.runtime = ConcourseRuntime.getRuntime();
        this.fromServer = new SharedMemory(fromServer);
        this.fromPlugin = new SharedMemory(fromPlugin);
        this.fromServerResponses = Maps
                .<AccessToken, RemoteMethodResponse> newConcurrentMap();
        Path logDir = Paths.get(System.getProperty(PLUGIN_HOME_JVM_PROPERTY)
                + File.separator + "log");
        logDir.toFile().mkdirs();
        this.log = Logger.builder().name(this.getClass().getName())
                .level(getConfig().getLogLevel()).directory(logDir.toString())
                .build();
    }

    /**
     * Start the plugin and process requests until instructed to
     * {@link Instruction#STOP stop}.
     */
    public void run() {
        beforeStart();
        log.info("Running plugin {}", this.getClass());
        ByteBuffer data;
        while ((data = fromServer.read()) != null) {
            Instruction type = ByteBuffers.getEnum(data, Instruction.class);
            data = ByteBuffers.getRemaining(data);
            if(type == Instruction.REQUEST) {
                RemoteMethodRequest request = Serializables.read(data,
                        RemoteMethodRequest.class);
                log.debug("Received REQUEST from Concourse Server: {}", request);
                Thread worker = new RemoteInvocationThread(request, fromPlugin,
                        this, false, fromServerResponses);
                worker.start();
            }
            else if(type == Instruction.RESPONSE) {
                RemoteMethodResponse response = Serializables.read(data,
                        RemoteMethodResponse.class);
                log.debug("Received RESPONSE from Concourse Server: {}",
                        response);
                ConcurrentMaps.putAndSignal(fromServerResponses,
                        response.creds, response);
            }
            else { // STOP
                beforeStop();
                log.info("Stopping plugin {}", this.getClass());
                break;
            }
        }
    }

    /**
     * A hook that is run once after the {@link Plugin} is installed.
     */
    protected void afterInstall() {}

    /**
     * A hook that is run every time before the {@link Plugin} {@link #run()
     * starts}.
     */
    protected void beforeStart() {}

    /**
     * A hook that is run every time before the {@link Plugin}
     * {@link Instruction#STOP stops}.
     */
    protected void beforeStop() {}

    /**
     * Return the {@link PluginConfiguration preferences} for this plugin.
     * <p>
     * The plugin should override this class if the
     * {@link StandardPluginConfiguration} is insufficient.
     * </p>
     * 
     * @return the {@link PluginConfiguration preferences} for the plugin
     */
    protected PluginConfiguration getConfig() {
        return new StandardPluginConfiguration();
    }

    /**
     * High level instructions that are communicated from Concourse Server to
     * the plugin via {@link #fromServer} channel.
     * 
     * @author Jeff Nelson
     */
    @PackagePrivate
    enum Instruction {
        MESSAGE, REQUEST, RESPONSE, STOP
    }

}
