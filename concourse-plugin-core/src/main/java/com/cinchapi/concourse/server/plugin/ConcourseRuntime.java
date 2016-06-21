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

import com.cinchapi.concourse.server.plugin.Plugin.Instruction;
import com.cinchapi.concourse.thrift.ComplexTObject;
import com.cinchapi.concourse.util.ByteBuffers;
import com.cinchapi.concourse.util.ConcurrentMaps;
import com.cinchapi.concourse.util.Serializables;
import com.google.common.base.Throwables;
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
    private <T> T invokeServer(String method, Object... args) {
        try {
            RemoteInvocationThread thread = (RemoteInvocationThread) Thread
                    .currentThread();
            List<ComplexTObject> targs = Lists
                    .newArrayListWithCapacity(args.length);
            for (Object arg : args) {
                targs.add(ComplexTObject.fromJavaObject(arg));
            }
            RemoteMethodRequest request = new RemoteMethodRequest(method,
                    thread.getAccessToken(), thread.getTransactionToken(),
                    thread.getEnvironment(), targs);
            ByteBuffer data0 = Serializables.getBytes(request);
            ByteBuffer data = ByteBuffer.allocate(data0.capacity() + 4);
            data.putInt(Instruction.REQUEST.ordinal());
            data.put(data0);
            thread.getRequestChannel().write(ByteBuffers.rewind(data));
            RemoteMethodResponse response = ConcurrentMaps.waitAndRemove(
                    thread.responses, thread.getAccessToken());
            if(!response.isError()) {
                return response.response.getJavaObject();
            }
            else {
                throw Throwables.propagate(response.error);
            }
        }
        catch (ClassCastException e) {
            throw new RuntimeException("Illegal attempt to use "
                    + getClass().getSimpleName()
                    + " from an unsupported thread");
        }
    }

}
