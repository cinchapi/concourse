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
package com.cinchapi.concourse.server.plugin.io;

import java.io.Serializable;

/**
 * A marker interface for objects that can be serialized and passed between
 * Concourse and plugin processes.
 * 
 * @author Jeff Nelson
 */
public interface PluginSerializable extends Serializable {
    // TODO (jnelson): In the future, we should use a different library (perhaps
    // create a custom one) to perform plugin serialization. Built-in java
    // serialization is incredibly inefficient and we stand to gain reduced
    // latency and increased performance if we take a more fine grained approach
    // to serializing data for plugins.
}
