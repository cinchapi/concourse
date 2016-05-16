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
import java.util.Arrays;
import java.util.List;

import javax.annotation.concurrent.Immutable;

import com.cinchapi.concourse.annotate.PackagePrivate;
import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.thrift.TransactionToken;

/**
 * A message that is sent from one process to another via a {@link SharedMemory}
 * segment with a request to invoke a remote method.
 * 
 * @author Jeff Nelson
 */
@Immutable
@PackagePrivate
final class RemoteMethodInvocation implements Serializable {

    /**
     * The serial version UID.
     */
    private static final long serialVersionUID = -4481493480486955070L;

    /**
     * The name of the method to invoke.
     */
    public final String method;

    /**
     * The credentials for the session that is making the request.
     */
    public final AccessToken creds;

    /**
     * The session's current transaction token.
     */
    public final TransactionToken transaction;

    /**
     * The session's current environment.
     */
    public final String environment;

    /**
     * The non-thrift arguments to pass to the method.
     */
    public final List<TObject> args;

    /**
     * Construct a new instance.
     * 
     * @param method
     * @param creds
     * @param transaction
     * @param environment
     * @param args
     */
    public RemoteMethodInvocation(String method, AccessToken creds,
            TransactionToken transaction, String environment, List<TObject> args) {
        this.method = method;
        this.creds = creds;
        this.transaction = transaction;
        this.environment = environment;
        this.args = args;
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
    public RemoteMethodInvocation(String method, AccessToken creds,
            TransactionToken transaction, String environment, TObject... args) {
        this(method, creds, transaction, environment, Arrays.asList(args));
    }
}
