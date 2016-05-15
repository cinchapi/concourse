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

import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.thrift.TransactionToken;

/**
 * 
 * 
 * @author Jeff Nelson
 */
public final class RemoteMethodInvocation implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = -4481493480486955070L;

    public final String method;
    public final AccessToken creds;
    public final TransactionToken transaction;
    public final String environment;
    public final List<TObject> args;

    public RemoteMethodInvocation(String method, AccessToken creds,
            TransactionToken transaction, String environment, List<TObject> args) {
        this.method = method;
        this.creds = creds;
        this.transaction = transaction;
        this.environment = environment;
        this.args = args;
    }

    public RemoteMethodInvocation(String method, AccessToken creds,
            TransactionToken transaction, String environment, TObject... args) {
        this(method, creds, transaction, environment, Arrays.asList(args));
    }
}
