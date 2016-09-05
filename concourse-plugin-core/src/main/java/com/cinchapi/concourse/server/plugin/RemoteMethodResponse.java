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

import java.io.Serializable;
import java.util.Map;

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
final class RemoteMethodResponse implements Serializable {

    // TODO how to represent errors/exceptions?

    /**
     * The serial version UID..
     */
    private static final long serialVersionUID = -7985973870612594547L;

    /**
     * The {@link AccessToken} of the session for which the response is routed.
     */
    public final AccessToken creds;

    /**
     * The response encapsulated as a thrift serializable object.
     */
    @Nullable
    public final ComplexTObject response;

    /**
     * The error that was thrown.
     */
    @Nullable
    public final Exception error;

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

}
