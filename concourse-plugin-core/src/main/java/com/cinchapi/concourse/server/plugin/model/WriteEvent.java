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
package com.cinchapi.concourse.server.plugin.model;

import java.util.Objects;

import javax.annotation.concurrent.Immutable;

import com.cinchapi.concourse.server.plugin.io.PluginSerializable;
import com.cinchapi.concourse.thrift.TObject;
import com.google.common.base.MoreObjects;

/**
 * A class that contains all the information about a single write event.
 */
@Immutable
public final class WriteEvent implements PluginSerializable {

    private static final long serialVersionUID = 7872986563642658668L;

    /**
     * An enum that describes the kind of write event this object represents.
     * 
     * @author Jeff Nelson
     */
    public enum Type implements PluginSerializable {
        ADD, REMOVE
    }

    /**
     * The key in which the event occurred.
     */
    private final String key;

    /**
     * The value for which the event occurred.
     */
    private final TObject value;

    /**
     * The record in which the event occurred.
     */
    private final long record;

    /**
     * The {@link Type} of the event.
     */
    private final Type type;

    /**
     * The timestamp at which the event occurred.
     */
    private final long timestamp;

    /**
     * environment associated with {@link Engine }
     */
    private final String environment;

    /**
     * Construct a new instance that is made up of {@link Write} and
     * {@code environment}
     *
     * @param write reference to the {@link Write} instance
     * @param environment environment associated with {@link Engine }
     */
    public WriteEvent(String key, TObject value, long record, long timestamp,
            Type type, String environment) {
        this.key = key;
        this.value = value;
        this.record = record;
        this.timestamp = timestamp;
        this.type = type;
        this.environment = environment;
    }

    /**
     * Return the {@link #key}.
     * 
     * @return the key
     */
    public String getKey() {
        return key;
    }

    /**
     * Return the {@link #value}.
     * 
     * @return the value
     */
    public TObject getValue() {
        return value;
    }

    /**
     * Return the {@link #record}.
     * 
     * @return the record
     */
    public long getRecord() {
        return record;
    }

    /**
     * Return the {@link #type}.
     * 
     * @return the type
     */
    public Type getType() {
        return type;
    }

    /**
     * Return the {@link #timestamp}.
     * 
     * @return the timestamp
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Return {@code environment} associated with {@link Engine}
     * 
     * @return the environment
     */
    public String getEnvironment() {
        return environment;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("key", key)
                .add("value", value).add("record", record).add("type", type)
                .add("timestamp", timestamp).add("environment", environment)
                .toString();
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof WriteEvent) {
            WriteEvent other = (WriteEvent) obj;
            return Objects.equals(key, other.key)
                    && Objects.equals(value, other.value)
                    && Objects.equals(record, other.record)
                    && Objects.equals(type, other.type)
                    && Objects.equals(timestamp, other.timestamp)
                    && Objects.equals(environment, other.environment);
        }
        else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value, record, type, timestamp, environment);
    }
}
