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

import java.util.Collections;
import java.util.List;

import com.cinchapi.concourse.server.plugin.io.PluginSerializable;
import com.cinchapi.concourse.server.plugin.model.WriteEvent;
import com.google.common.collect.Lists;

/**
 * A a chronological sequence of {@link WriteEvent WriteEvents} that is
 * periodically streamed to {@link RealTimePlugin real time plugins}.
 * 
 * @author Jeff Nelson
 */
public class Packet implements PluginSerializable {

    private static final long serialVersionUID = 9214118090555607982L;

    /**
     * All the {@link WriteEvent WriteEvents} in this packet.
     */
    private final List<WriteEvent> events;

    /**
     * Construct a new instance.
     */
    public Packet() {
        this.events = Lists.newArrayList();
    }

    /**
     * Return the {@link WriteEvent events} in this Packet as a sequential list.
     * 
     * @return the Packet's events
     */
    public List<WriteEvent> events() {
        return Collections.unmodifiableList(events);
    }

}
