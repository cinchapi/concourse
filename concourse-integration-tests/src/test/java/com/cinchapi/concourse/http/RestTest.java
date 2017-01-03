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
package com.cinchapi.concourse.http;

import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.DataServices;

/**
 * A base class for tests against the REST Api. This class handles boilerplate
 * scaffolding (e.g. logging in, etc) so that subclasses can simply define test
 * logic.
 * 
 * @author Jeff Nelson
 */
public class RestTest extends HttpTest {

    @Override
    public void beforeEachTest() {
        super.beforeEachTest();
        login();
    }

    /**
     * Return a string that encodes {@code value} in the form that is necessary
     * for JSON import.
     * 
     * @param value
     * @return the json import ready string
     */
    public static String prepareForJsonImport(Object value) {
        return DataServices.gson().toJson(Convert.javaToThrift(value));
    }

}
