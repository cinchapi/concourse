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

/**
 * A {@link RuntimeException} that indicates an error occurred with a plugin
 * operation.
 * 
 * @author Jeff Nelson
 */
public class PluginException extends RuntimeException {

    /**
     * Construct a new instance.
     * 
     * @param message
     */
    public PluginException(String message) {
        super(message);
    }

    private static final long serialVersionUID = 1L;

}
