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
import io.atomix.catalyst.buffer.HeapBuffer;

import javax.annotation.concurrent.Immutable;

/**
 * 
 * 
 * @author Jeff Nelson
 */
@Immutable
public abstract class RemoteMessage {

    @SuppressWarnings("unchecked")
    public static <T extends RemoteMessage> T fromBuffer(Buffer buffer) {
        Type type = Type.values()[buffer.readByte()];
        T message = null;
        if(type == Type.ATTRIBUTE) {
            message = (T) new RemoteAttributeExchange();
        }
        else if(type == Type.REQUEST) {
            message = (T) new RemoteMethodRequest();
        }
        else if(type == Type.RESPONSE) {
            message = (T) new RemoteMethodResponse();
        }
        else if(type == Type.STOP) {
            message = (T) new RemoteStopRequest();
        }
        else {
            throw new IllegalArgumentException("Unrecognized "
                    + RemoteMessage.class.getSimpleName() + " type");
        }
        message.deserialize(buffer);
        return message;
    }

    public abstract void deserialize(Buffer buffer);

    public final Buffer serialize() {
        Buffer buffer = HeapBuffer.allocate();
        buffer.writeByte(type().ordinal());
        serialize(buffer);
        buffer.flip();
        return buffer;
    }

    protected abstract void serialize(Buffer buffer);

    public abstract Type type();

    enum Type {
        ATTRIBUTE, REQUEST, RESPONSE, STOP
    }

}
