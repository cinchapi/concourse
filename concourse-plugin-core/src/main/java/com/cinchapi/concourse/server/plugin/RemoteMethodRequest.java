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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.concurrent.Immutable;

import com.cinchapi.concourse.annotate.PackagePrivate;
import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.thrift.ComplexTObject;
import com.cinchapi.concourse.thrift.TransactionToken;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * A message that is sent from one process to another via a
 * {@link InterProcessCommunication} segment with a request to invoke a remote
 * method.
 * 
 * @author Jeff Nelson
 */
@Immutable
@PackagePrivate
final class RemoteMethodRequest extends RemoteMessage {

    /**
     * The non-thrift arguments to pass to the method.
     */
    public List<ComplexTObject> args;

    /**
     * The credentials for the session that is making the request.
     */
    public AccessToken creds;

    /**
     * The session's current environment.
     */
    public String environment;

    /**
     * The name of the method to invoke.
     */
    public String method;

    /**
     * The session's current transaction token.
     */
    public TransactionToken transaction;

    /**
     * Construct a new instance.
     * 
     * @param method
     * @param creds
     * @param transaction
     * @param environment
     * @param args
     */
    public RemoteMethodRequest(String method, AccessToken creds,
            TransactionToken transaction, String environment,
            ComplexTObject... args) {
        this(method, creds, transaction, environment, Arrays.asList(args));
    }

    /**
     * Construct a new instance.
     * 
     * @param method
     * @param creds
     * @param transaction
     * @param environment
     * @param args
     */
    public RemoteMethodRequest(String method, AccessToken creds,
            TransactionToken transaction, String environment,
            List<ComplexTObject> args) {
        this.method = method;
        this.creds = creds;
        this.transaction = transaction;
        this.environment = environment;
        this.args = args;
    }

    /**
     * DO NOT CALL. Only here for deserialization.
     */
    RemoteMethodRequest() {/* no-op */}

    @Override
    public void deserialize(Buffer buffer) {
        this.method = buffer.readUTF8();
        byte[] creds0 = new byte[buffer.readInt()];
        buffer.read(creds0);
        this.creds = new AccessToken(ByteBuffer.wrap(creds0));
        boolean transaction0 = buffer.readByte() == 1 ? true : false;
        this.transaction = null;
        if(transaction0) {
            long timestamp = buffer.readLong();
            this.transaction = new TransactionToken(creds, timestamp);
        }
        this.environment = buffer.readUTF8();
        this.args = Lists.newArrayList();
        while (buffer.hasRemaining()) {
            int length = buffer.readInt();
            byte[] arg = new byte[length];
            buffer.read(arg);
            args.add(ComplexTObject.fromByteBuffer(ByteBuffer.wrap(arg)));
        }

    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof RemoteMethodRequest) {
            return Objects.equals(args, ((RemoteMethodRequest) obj).args)
                    && Objects.equals(creds, ((RemoteMethodRequest) obj).creds)
                    && Objects.equals(environment,
                            ((RemoteMethodRequest) obj).environment)
                    && Objects.equals(method,
                            ((RemoteMethodRequest) obj).method)
                    && Objects.equals(transaction,
                            ((RemoteMethodRequest) obj).transaction);
        }
        else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(args, creds, environment, method, transaction);
    }

    @Override
    public String toString() {
        Map<String, Object> data = Maps.newHashMap();
        data.put("method", method);
        data.put("args", args);
        data.put("creds", creds);
        data.put("transaction", transaction);
        data.put("environment", environment);
        return data.toString();
    }

    @Override
    public Type type() {
        return Type.REQUEST;
    }

    @Override
    protected void serialize(Buffer buffer) {
        buffer.writeUTF8(method);
        byte[] creds = this.creds.getData();
        buffer.writeInt(creds.length);
        buffer.write(creds);
        buffer.writeBoolean(transaction != null);
        if(transaction != null) {
            buffer.writeLong(transaction.getTimestamp());
            // NOTE: Will need the AccessToken to re-create the
            // TransactionToken, but there is no need to re-write it since it
            // was previously written
        }
        buffer.writeUTF8(environment);
        args.forEach((arg) -> {
            ByteBuffer bytes = arg.toByteBuffer();
            buffer.writeInt(bytes.remaining());
            buffer.write(bytes.array());
        });
    }
}
