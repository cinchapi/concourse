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
package com.cinchapi.concourse.server.plugin;

import io.atomix.catalyst.buffer.Buffer;

import java.util.Collections;
import java.util.List;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.server.plugin.data.WriteEvent;
import com.cinchapi.concourse.server.plugin.io.PluginSerializable;
import com.google.common.collect.Lists;

/**
 * A a chronological sequence of {@link WriteEvent WriteEvents} that is
 * periodically streamed to {@link RealTimePlugin real time plugins}.
 * 
 * @author Jeff Nelson
 */
public class Packet implements PluginSerializable {

    /**
     * All the {@link WriteEvent WriteEvents} in this packet.
     */
    private final List<WriteEvent> events;

    /**
     * DO NOT CALL. Used for deserializaton.
     */
    @SuppressWarnings("unused")
    private Packet() {
        this(Lists.newArrayList());
    }

    /**
     * Construct a new instance.
     *
     * @param events - collection of {@link WriteEvent WriteEvents}
     */
    public Packet(List<WriteEvent> events) {
        this.events = events;
    }

    @Override
    public void deserialize(Buffer buffer) {
        if(events.isEmpty()) {
            while (buffer.hasRemaining()) {
                WriteEvent event = Reflection.newInstance(WriteEvent.class);
                event.deserialize(buffer);
                events.add(event);
            }
        }
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Packet) {
            return events.equals(((Packet) obj).events);
        }
        else {
            return false;
        }
    }

    /**
     * Return the {@link WriteEvent events} in this Packet as a sequential list.
     *
     * @return the Packet's events
     */
    public List<WriteEvent> events() {
        return Collections.unmodifiableList(events);
    }

    @Override
    public int hashCode() {
        return events.hashCode();
    }

    @Override
    public void serialize(Buffer buffer) {
        events.forEach((item) -> item.serialize(buffer));
    }

    @Override
    public String toString() {
        return events.toString();
    }

}
