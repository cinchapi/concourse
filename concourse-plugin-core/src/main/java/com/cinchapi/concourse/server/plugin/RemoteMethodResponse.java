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

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.cinchapi.concourse.annotate.PackagePrivate;
import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.thrift.ComplexTObject;
import com.google.common.collect.Maps;

/**
 * A message that is sent from one process to another via a {@link SharedMemory}
 * segment with the result of a {@link RemoteMethodRequest}.
 * 
 * @author Jeff Nelson
 */
@Immutable
@PackagePrivate
final class RemoteMethodResponse extends RemoteMessage {

    /**
     * The {@link AccessToken} of the session for which the response is routed.
     */
    public AccessToken creds;

    /**
     * The error that was thrown.
     */
    @Nullable
    public Exception error;

    /**
     * The response encapsulated as a thrift serializable object.
     */
    @Nullable
    public ComplexTObject response;

    /**
     * Construct a new instance.
     * 
     * @param creds
     * @param response
     */
    public RemoteMethodResponse(AccessToken creds, ComplexTObject response) {
        this.creds = creds;
        this.response = response;
        this.error = null;
    }

    /**
     * Construct a new instance.
     * 
     * @param creds
     * @param error
     */
    public RemoteMethodResponse(AccessToken creds, Exception error) {
        this.creds = creds;
        this.response = null;
        this.error = error;
    }

    /**
     * DO NOT CALL. Only here for deserialization.
     */
    RemoteMethodResponse() {/* no-op */}

    @Override
    public void deserialize(Buffer buffer) {
        boolean isError = buffer.readBoolean();
        int credsLength = buffer.readInt();
        byte[] creds = new byte[credsLength];
        buffer.read(creds);
        this.creds = new AccessToken(ByteBuffer.wrap(creds));
        if(isError) {
            this.error = new RuntimeException(buffer.readUTF8());
        }
        else {
            byte[] response = new byte[(int) buffer.remaining()];
            buffer.read(response);
            this.response = ComplexTObject.fromByteBuffer(ByteBuffer
                    .wrap(response));
        }
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof RemoteMethodResponse) {
            return Objects.equals(creds, ((RemoteMethodResponse) obj).creds)
                    && error == null ? Objects.equals(response,
                    ((RemoteMethodResponse) obj).response) : Objects.equals(
                    error, ((RemoteMethodResponse) obj).error);
        }
        else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(creds, error == null ? response : error);
    }

    /**
     * Return {@code true} if this {@link RemoteMethodResponse response}
     * indicates an error.
     * 
     * @return {@code true} if this is an error response
     */
    public boolean isError() {
        return error != null;
    }

    @Override
    public String toString() {
        Map<String, Object> data = Maps.newHashMap();
        data.put("error", isError());
        data.put("response", isError() ? error : response);
        data.put("creds", creds);
        return data.toString();
    }

    @Override
    public Type type() {
        return Type.RESPONSE;
    }

    @Override
    protected void serialize(Buffer buffer) {
        buffer.writeBoolean(isError());
        byte[] creds = this.creds.getData();
        buffer.writeInt(creds.length);
        buffer.write(creds);
        if(isError()) {
            buffer.writeUTF8(error.getMessage());
        }
        else {
            buffer.write(response.toByteBuffer().array());
        }
    }
}
