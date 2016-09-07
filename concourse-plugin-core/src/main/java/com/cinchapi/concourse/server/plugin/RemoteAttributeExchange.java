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

import javax.annotation.concurrent.Immutable;

import com.cinchapi.concourse.annotate.PackagePrivate;

import io.atomix.catalyst.buffer.Buffer;

/**
 * A {@link RemoteMessage message} that exchanges an attribute between two
 * remote processes.
 * 
 * @author Jeff Nelson
 */
@Immutable
@PackagePrivate
final class RemoteAttributeExchange extends RemoteMessage {

    /**
     * Construct a new instance.
     * 
     * @param key
     * @param value
     */
    public RemoteAttributeExchange(String key, String value) {
        this.key = key;
        this.value = value;
    }

    /**
     * DO NOT CAL. Only here for deserialization.
     */
    RemoteAttributeExchange() {/* no-op */}

    /**
     * The attribute's key.
     */
    private String key;

    /**
     * The attribute's value.
     */
    private String value;

    @Override
    public void deserialize(Buffer buffer) {
        this.key = buffer.readUTF8();
        this.value = buffer.readUTF8();
    }

    @Override
    protected void serialize(Buffer buffer) {
        buffer.writeUTF8(key);
        buffer.writeUTF8(value);
    }

    @Override
    public Type type() {
        return Type.ATTRIBUTE;
    }

}
