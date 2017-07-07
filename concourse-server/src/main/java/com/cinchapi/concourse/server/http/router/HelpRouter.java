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
package com.cinchapi.concourse.server.http.router;

import com.cinchapi.concourse.server.ConcourseServer;
import com.cinchapi.concourse.server.http.Endpoint;
import com.cinchapi.concourse.server.http.EndpointContainer;
import com.cinchapi.concourse.server.http.HttpRequest;
import com.cinchapi.concourse.server.http.HttpResponse;
import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.thrift.TransactionToken;
import com.google.gson.JsonPrimitive;

/**
 * 
 * 
 * @author Jeff Nelson
 */
public class HelpRouter extends EndpointContainer {

    /**
     * Construct a new instance.
     * 
     * @param concourse
     */
    public HelpRouter(ConcourseServer concourse) {
        super(concourse);
    }

    public Endpoint get = new Endpoint() {

        @Override
        public String serve(HttpRequest request, HttpResponse response,
                AccessToken creds, TransactionToken transaction,
                String environment) throws Exception {
            return new JsonPrimitive("This is where the help goes").toString();
        }

    };

}
