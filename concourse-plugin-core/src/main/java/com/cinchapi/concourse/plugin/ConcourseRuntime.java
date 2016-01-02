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
package com.cinchapi.concourse.plugin;

import com.cinchapi.concourse.thrift.ConcourseService;

/**
 * A {@code ConcourseRuntime} is the backend of a Concourse deployment.
 * Every plugin is given access to a Runtime in order to interact with the
 * underlying system.
 * 
 * @author Jeff Nelson
 */
public interface ConcourseRuntime extends ConcourseService.Iface {

    /**
     * Return the underlying {@link Storage} that is used by the default
     * environment in this {@code ConcourseRuntime}.
     * 
     * @return the underlying {@link Storage}
     */
    public Storage getStorage();

    /**
     * Return the underlying {@link Storage} that is used by the specified
     * {@code environment} in this {@code ConcourseRuntime}.
     * 
     * @param environment
     * @return the underlying {@link Storage} for the {@code environment}
     */
    public Storage getStorage(String environment);

}
