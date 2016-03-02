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
package com.cinchapi.concourse.plugin.http;

/**
 * An interface for objects that contain logic for plugin functionality that can
 * be invoked via HTTP.
 * 
 * @author Jeff Nelson
 */
public interface HttpCallable {

    /**
     * Set the path for this {@link HttpCallable callable}. This method should
     * only be called when initializing.
     * 
     * @param path the absolute path for the callable
     */
    public void setPath(String path);

    /**
     * Set the HTTP verb/action that must be used in order to access this
     * callable at the appropriate {@link #setPath(String) path}.
     * 
     * @param action an HTTP verb (GET, POST, PUT, DELETE, UPSERT)
     */
    public void setAction(String action);

    /**
     * Return the HTTP verb/action to which this endpoint responds.
     * <p>
     * This value is configured using {@link #setAction(String)}.
     * </p>
     * 
     * @return the action for this Endpoint
     */
    public String getAction();
}
