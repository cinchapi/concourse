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
package com.cinchapi.concourse.server.plugin.data;

import io.atomix.catalyst.buffer.Buffer;

import java.nio.ByteBuffer;
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

    /**
     * An enum that describes the kind of write event this object represents.
     * 
     * @author Jeff Nelson
     */
    public enum Type {
        ADD, REMOVE
    }

    /**
     * The key in which the event occurred.
     */
    private String key;

    /**
     * The value for which the event occurred.
     */
    private TObject value;

    /**
     * The record in which the event occurred.
     */
    private long record;

    /**
     * The {@link Type} of the event.
     */
    private Type type;

    /**
     * The timestamp at which the event occurred.
     */
    private long timestamp;

    /**
     * environment associated with {@link Engine }
     */
    private String environment;

    /**
     * DO NOT CALL. Used for deserializaton.
     */
    @SuppressWarnings("unused")
    private WriteEvent() {/* no-op */}

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

    @Override
    public void serialize(Buffer buffer) {
        buffer.writeUTF8(key);
        buffer.writeByte(value.type.ordinal());
        byte[] valueData = value.getData();
        buffer.writeInt(valueData.length);
        buffer.write(valueData);
        buffer.writeLong(record);
        buffer.writeByte(type.ordinal());
        buffer.writeLong(timestamp);
        buffer.writeUTF8(environment);

    }

    @Override
    public void deserialize(Buffer buffer) {
        key = buffer.readUTF8();
        com.cinchapi.concourse.thrift.Type ttype = com.cinchapi.concourse.thrift.Type
                .values()[buffer.readByte()];
        byte[] valueData = new byte[buffer.readInt()];
        buffer.read(valueData);
        value = new TObject(ByteBuffer.wrap(valueData), ttype);
        record = buffer.readLong();
        type = Type.values()[buffer.readByte()];
        timestamp = buffer.readLong();
        environment = buffer.readUTF8();
    }
}
