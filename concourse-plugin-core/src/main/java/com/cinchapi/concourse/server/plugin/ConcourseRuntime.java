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

import java.nio.ByteBuffer;
import java.util.List;
import com.cinchapi.concourse.server.plugin.io.SharedMemory;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.util.ByteBuffers;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.Serializables;
import com.google.common.collect.Lists;

/**
 * {@link ConcourseRuntime} is an interface to a local Concourse Server node.
 * This class can be used within {@link Plugin plugins} to invoke publicly
 * declared methods that are defined on the server.
 * 
 * @author Jeff Nelson
 */
public final class ConcourseRuntime {

    /**
     * Singleton instance so that multiple plugins in the same distribution do
     * not unnecessarily create multiple runtime objects.
     */
    private static final ConcourseRuntime INSTANCE = new ConcourseRuntime();

    /**
     * Return the runtime instance associated with the current plugin.
     * 
     * @return the {@link ConcourseRuntime} instance associated with the current
     *         plugin
     */
    public static ConcourseRuntime getRuntime() {
        return INSTANCE;
    }

    /**
     * Construct a new instance.
     */
    private ConcourseRuntime() { /* noop */
        // NOTE: Routing to the correct Concourse Server instance is handled via
        // communication channels stored in each thread that accesses this
        // instance.
    }

    public String getServerEnvironment() {
        return invokeServer("getServerEnvironment");
    }

    /**
     * Invoke {@code method} with {@code args} on the local Concourse Server
     * instance that is associated with this {@link ConcourseRuntime runtime}.
     * 
     * @param method the name of the method to invoke
     * @param args the args to pass to the method
     * @return the result of the method invocation
     */
    @SuppressWarnings("unchecked")
    private <T> T invokeServer(String method, Object... args) {
        try {
            RemoteInvocationListenerThread thread = (RemoteInvocationListenerThread) Thread
                    .currentThread();
            List<TObject> targs = Lists.newArrayListWithCapacity(args.length);
            for (Object arg : args) {
                targs.add(Convert.javaToThrift(arg));
            }
            RemoteMethodInvocation remote = new RemoteMethodInvocation(method,
                    thread.getAccessToken(), thread.getTransactionToken(),
                    thread.getEnvironment(), targs);
            SharedMemory outbox = thread.getOutgoingChannel();
            outbox.write(Serializables.getBytes(remote));
            ByteBuffer data = outbox.read();
            RemoteMethodResponse responseStruct = Serializables.read(
                    ByteBuffers.rewind(data), RemoteMethodResponse.class);
            T response = (T) Convert.thriftToJava(responseStruct.response);
            return response;
        }
        catch (ClassCastException e) {
            throw new RuntimeException("Illegal attempt to use "
                    + getClass().getSimpleName()
                    + " from an unsupported thread");
        }
    }

}
