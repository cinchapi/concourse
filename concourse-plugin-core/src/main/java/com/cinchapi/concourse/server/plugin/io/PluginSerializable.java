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
package com.cinchapi.concourse.server.plugin.io;

import io.atomix.catalyst.buffer.Buffer;

/**
 * A marker interface for non-thrift objects that can be serialized and passed
 * between Concourse and plugin processes.
 * 
 * @author Jeff Nelson
 */
public interface PluginSerializable {

    /**
     * Return the object's contents from its serialized form within the
     * {@code buffer}.
     * 
     * @param buffer a {@link Buffer} for reading bytes
     */
    public void deserialize(Buffer buffer);

    /**
     * Write the serialized form of the object to the {@code buffer}.
     * 
     * @param buffer a {@link Buffer} for writing bytes
     */
    public void serialize(Buffer buffer);

}
