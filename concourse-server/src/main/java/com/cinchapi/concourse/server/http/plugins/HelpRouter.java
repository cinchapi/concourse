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
package com.cinchapi.concourse.server.http.plugins;

import spark.Request;
import spark.Response;

import com.cinchapi.concourse.plugin.http.HttpPlugin;
import com.cinchapi.concourse.server.ConcourseServer;
import com.cinchapi.concourse.server.http.Resource;
import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.thrift.TransactionToken;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

/**
 * 
 * 
 * @author Jeff Nelson
 */
public class HelpRouter extends HttpPlugin {

    /**
     * Construct a new instance.
     * 
     * @param concourse
     */
    public HelpRouter(ConcourseServer concourse) {
        super(concourse);
    }

    public Resource get = new Resource() {

        @Override
        protected JsonElement serve(Request request, Response response,
                AccessToken creds, TransactionToken transaction,
                String environment) throws Exception {
            return new JsonPrimitive("This is where the help goes");
        }

    };

}
