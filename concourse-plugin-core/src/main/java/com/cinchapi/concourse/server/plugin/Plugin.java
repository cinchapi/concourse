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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.concurrent.Immutable;

import org.apache.commons.io.output.TeeOutputStream;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.common.logging.Logger;
import com.cinchapi.concourse.server.plugin.io.PluginSerializer;
import com.cinchapi.concourse.server.plugin.io.SharedMemory;
import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.util.ConcurrentMaps;
import com.google.common.collect.Maps;
import com.google.common.io.BaseEncoding;

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
     * The {@link AccessToken} that the plugin should use when making non-user
     * (i.e. service) requests to Concourse Server.
     */
    public final static AccessToken SERVICE_TOKEN;

    /**
     * The name of the dynamic property that is passed to the plugin's JVM to
     * instruct it as to where the plugin's home is located.
     */
    protected final static String PLUGIN_HOME_JVM_PROPERTY = "com.cinchapi.concourse.plugin.home";

    /**
     * The name of the dynamic property that is passed to the plugin's JVM to
     * instruct it as to what {@link AccessToken} to use for service-based
     * server requests.
     */
    protected final static String PLUGIN_SERVICE_TOKEN_JVM_PROPERTY = "com.cinchapi.concourse.plugin.token";
    static {
        // Read the service token from the system properties
        String encoded = System.getProperty(PLUGIN_SERVICE_TOKEN_JVM_PROPERTY);
        if(encoded != null) {
            byte[] decoded = BaseEncoding.base32Hex().decode(encoded);
            ByteBuffer bytes = ByteBuffer.wrap(decoded);
            SERVICE_TOKEN = new AccessToken(bytes);
        }
        else {
            SERVICE_TOKEN = null;
        }
    }

    /**
     * The communication channel for messages that come from Concourse Server,
     */
    protected final SharedMemory fromServer;

    /**
     * A {@link Logger} for plugin operations.
     */
    protected final Logger log;

    /**
     * A reference to the local Concourse Server {@link ConcourseRuntime
     * runtime} to which this plugin is registered.
     */
    protected final ConcourseRuntime runtime;

    /**
     * Responsible for taking arbitrary objects and turning them into binary so
     * they can be sent across the wire.
     */
    protected final PluginSerializer serializer = new PluginSerializer();

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
        
        // Redirect System.out and System.err to a console.log file
        Path consoleLog = logDir.resolve("console.log");
        try {
            File consoleLogFile = consoleLog.toFile();
            consoleLogFile.createNewFile();
            FileOutputStream fos = new FileOutputStream(consoleLogFile);
            TeeOutputStream out = new TeeOutputStream(System.out, fos);
            TeeOutputStream err = new TeeOutputStream(System.err, fos);
            System.setOut(new PrintStream(out));
            System.setErr(new PrintStream(err));
        }
        catch (IOException e) {
            throw CheckedExceptions.throwAsRuntimeException(e);
        }
    }

    /**
     * Return a {@link BackgroundInformation} instance that has plugin-related
     * attributes that are needed for making background requests to the upstream
     * service.
     * 
     * @return the Plugin's {@link BackgroundInformation}.
     */
    public BackgroundInformation backgroundInformation() {
        return new BackgroundInformation();
    }

    /**
     * Start the plugin and process requests until instructed to
     * {@link Instruction#STOP stop}.
     */
    public void run() {
        log.info("Running plugin {}", this.getClass());
        ByteBuffer data;
        while ((data = fromServer.read()) != null) {
            RemoteMessage message = serializer.deserialize(data);
            if(message.type() == RemoteMessage.Type.REQUEST) {
                RemoteMethodRequest request = (RemoteMethodRequest) message;
                log.debug("Received REQUEST from Concourse Server: {}",
                        message);
                Thread worker = new RemoteInvocationThread(request, fromPlugin,
                        this, false, fromServerResponses);
                worker.setUncaughtExceptionHandler((thread, throwable) -> {
                    log.error(
                            "While processing request '{}', the following "
                                    + "non-recoverable error occurred:",
                            request, throwable);
                });
                worker.start();
            }
            else if(message.type() == RemoteMessage.Type.RESPONSE) {
                RemoteMethodResponse response = (RemoteMethodResponse) message;
                log.debug("Received RESPONSE from Concourse Server: {}",
                        response);
                ConcurrentMaps.putAndSignal(fromServerResponses, response.creds,
                        response);
            }
            else if(message.type() == RemoteMessage.Type.STOP) { // STOP
                log.info("Stopping plugin {}", this.getClass());
                break;
            }
            else {
                // Ignore the message...
                continue;
            }
        }
    }

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
     * A wrapper class for all the information needed to perform background
     * requests in this Plugin and its related classes.
     * 
     * @author Jeff Nelson
     */
    @Immutable
    public class BackgroundInformation {

        private BackgroundInformation() {/* no-op */}

        /**
         * Return the {@link SharedMemory} channel that the Plugin and its
         * related classes use for outgoing messages to the upstream service.
         * 
         * @return the outgoing channel
         */
        public SharedMemory outgoing() {
            return fromPlugin;
        }

        /**
         * Return the queue of responses from the upstream service.
         * 
         * @return the response queue
         */
        public ConcurrentMap<AccessToken, RemoteMethodResponse> responses() {
            return fromServerResponses;
        }
    }

}
