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

import java.util.Objects;

import io.atomix.catalyst.buffer.Buffer;

/**
 * A {@link RemoteMessage message} that tells a remote process to stop.
 * 
 * @author Jeff Nelson
 */
public class RemoteStopRequest extends RemoteMessage {

    /**
     * Construct a new instance.
     */
    public RemoteStopRequest() {/* no-op */}

    @Override
    public void deserialize(Buffer buffer) {/* no-op */}

    @Override
    protected void serialize(Buffer buffer) {/* no-op */}

    @Override
    public Type type() {
        return Type.STOP;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type());
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof RemoteMessage) {
            return type() == ((RemoteMessage) obj).type();
        }
        else {
            return false;
        }
    }

}
