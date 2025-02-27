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
package com.cinchapi.concourse.server.gossip;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.ensemble.gossip.Gossip;
import com.cinchapi.ensemble.io.Serialization;

/**
 * {@link StartEngineGossip} is a message that instructs other nodes to
 * start the engine for the specified environment.
 *
 * @author Jeff Nelson
 */
public class StartEngineGossip extends Gossip {

    private static UUID generateUUID(String environment) {
        String name = AnyStrings.format("{} - {}",
                StartEngineGossip.class.getName(), environment);
        byte[] bytes = name.getBytes(StandardCharsets.UTF_8);
        return UUID.nameUUIDFromBytes(bytes);
    }

    /**
     * The name of the environment for which the engine should be started.
     */
    private String environment;

    /**
     * Construct a new instance.
     * 
     * @param environment
     */
    public StartEngineGossip(String environment) {
        super(generateUUID(environment));
        this.environment = environment;
    }

    /**
     * Construct a new instance.
     * 
     * @param id
     */
    public StartEngineGossip(UUID id) {
        super(id);
    }

    /**
     * Return the name of the environment for which the engine should be
     * started.
     *
     * @return the environment name
     */
    public String environment() {
        return environment;
    }

    @Override
    protected void readFrom(Serialization stream) {
        environment = stream.readUtf8();

    }

    @Override
    protected void writeTo(Serialization stream) {
        stream.writeUtf8(environment);
    }

    @Override
    public String toString() {
        return "Start Engine "+environment;
    }
    
    

}
