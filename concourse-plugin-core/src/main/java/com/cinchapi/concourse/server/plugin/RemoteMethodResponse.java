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

import javax.annotation.concurrent.Immutable;

import com.cinchapi.concourse.annotate.PackagePrivate;
import com.cinchapi.concourse.thrift.TObject;

/**
 * A message that is sent from one process to another via a {@link SharedMemory}
 * segment with the result of a {@link RemoteMethodInvocation}.
 * 
 * @author Jeff Nelson
 */
@Immutable
@PackagePrivate
final class RemoteMethodResponse implements Serializable {

    /**
     * The serial version UID..
     */
    private static final long serialVersionUID = -7985973870612594547L;

    /**
     * The response encapsulated as a thrift serializable object.
     */
    public final TObject response;

    /**
     * Construct a new instance.
     * 
     * @param response
     */
    public RemoteMethodResponse(TObject response) {
        this.response = response;
    }

}
