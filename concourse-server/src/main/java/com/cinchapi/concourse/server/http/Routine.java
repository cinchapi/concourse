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
package com.cinchapi.concourse.server.http;

import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.thrift.TransactionToken;

import spark.Request;
import spark.Response;

/**
 * A {@link Routine} is a {@link Route} that does not return a payload or render
 * a view. It is generally used to check for some common preconditions before
 * executing other routes.
 * <p>
 * A Routine matches every other route for the Router in which it was defined.
 * For example, a Routine defined in the {@code HelloWorldRouter} will match all
 * requests to {@code /hello/world/*}. Routines are meant to be catch-alls so
 * there is no way to further specify the paths a Routine should match.
 * <p>
 * 
 * @author Jeff Nelson
 */
public abstract class Routine extends Endpoint {


    @Override
    protected final Object handle(Request request, Response response,
            AccessToken creds, TransactionToken transaction, String environment) {
        run(request, response, creds, transaction, environment);
        return "";
    }

    /**
     * Run the routine. If, for some reason, the routine fails, you may call
     * {@link #halt()}, or redirect to another route or throw an exception.
     */
    protected abstract void run(Request request, Response response,
            AccessToken creds, TransactionToken transaction, String environment);

}
