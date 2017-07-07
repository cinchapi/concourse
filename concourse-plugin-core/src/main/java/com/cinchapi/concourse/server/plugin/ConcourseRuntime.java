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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.LogManager;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.Language;
import com.cinchapi.concourse.server.plugin.data.ObjectResultDataset;
import com.cinchapi.concourse.server.plugin.data.TObjectResultDataset;
import com.cinchapi.concourse.server.plugin.io.PluginSerializer;
import com.cinchapi.concourse.thrift.ComplexTObject;
import com.cinchapi.concourse.util.ConcurrentMaps;
import com.cinchapi.concourse.util.Convert;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.matcher.Matchers;

/**
 * Every Plugin application has a single instance of the
 * {@link ConcourseRuntime} class that allows the Plugin to interact with the
 * local Concourse Server node from which the Plugin was launched.
 * 
 * <p>
 * The current runtime can be obtained using provided the {@link #getRuntime()}
 * method. For convenience, each {@link Plugin} provides access to the runtime
 * via the {@link Plugin#runtime} variable which is available to all
 * implementing applications.
 * </p>
 * 
 * @author Jeff Nelson
 */
public class ConcourseRuntime extends StatefulConcourseService {

    static {
        // turn off logging from java.util.logging
        LogManager.getLogManager().reset();
    }

    /**
     * Responsible for taking arbitrary objects and turning them into binary so
     * they can be sent across the wire.
     */
    private static PluginSerializer serializer = new PluginSerializer();

    /**
     * Return the runtime instance associated with the current plugin.
     * 
     * @return the {@link ConcourseRuntime} instance associated with the current
     *         plugin
     */
    @NoGuice
    public static ConcourseRuntime getRuntime() {
        return INSTANCE;
    }

    /**
     * Return a {@link ConcourseRuntime} that is configured to intercept method
     * calls and proxy them through the {@link #invokeServer(String, Object...)}
     * method.
     * 
     * @return a {@link ConcourseRuntime} instance
     */
    @NoGuice
    private static ConcourseRuntime init() {
        Injector injector = Guice.createInjector(new ServerInvokerModule());
        ConcourseRuntime runtime = injector.getInstance(ConcourseRuntime.class);
        return runtime;
    }

    /**
     * Invoke {@code method} with {@code args} on the local Concourse Server
     * instance that is associated with this {@link ConcourseRuntime runtime}.
     * 
     * @param method the name of the method to invoke
     * @param args the args to pass to the method
     * @return the result of the method invocation
     */
    @NoGuice
    @SuppressWarnings("unchecked")
    private static <T> T invokeServer(String method, Object... args) {
        try {
            ConcourseRuntimeAuthorized thread = (ConcourseRuntimeAuthorized) Thread
                    .currentThread();
            List<ComplexTObject> targs = Lists
                    .newArrayListWithCapacity(args.length);
            Collection<Integer> valueTransform = VALUE_TRANSFORM.get(method);
            Collection<Integer> criteriaTransform = CRITERIA_TRANSFORM
                    .get(method);
            for (int i = 0; i < args.length; ++i) {
                // Must go through each parameters and transform generic value
                // objects into TObjects and all Criteria into TCriteria.
                Object arg = args[i];
                if(valueTransform.contains(i)) {
                    if(arg instanceof List) {
                        arg = Convert.javaListToThrift((List<Object>) arg);
                    }
                    else if(arg instanceof Set) {
                        arg = Convert.javaSetToThrift((Set<Object>) arg);
                    }
                    else if(arg instanceof Map) {
                        arg = Convert.javaMapToThrift((Map<?, Object>) arg);
                    }
                    else {
                        arg = Convert.javaToThrift(arg);
                    }
                }
                else if(criteriaTransform.contains(i)) {
                    arg = Language.translateToThriftCriteria((Criteria) arg);
                }
                targs.add(ComplexTObject.fromJavaObject(arg));
            }
            // Send a RemoteMethodRequest to the server, asking that the locally
            // invoked method be executed. The result will be placed on the
            // current thread's response queue
            RemoteMethodResponse response;
            synchronized (thread.accessToken()) {
                RemoteMethodRequest request = new RemoteMethodRequest(method,
                        thread.accessToken(), thread.transactionToken(),
                        thread.environment(), targs);
                ByteBuffer buffer = serializer.serialize(request);
                thread.outgoing().write(buffer);
                response = ConcurrentMaps.waitAndRemove(thread.responses(),
                        thread.accessToken());
            }
            if(!response.isError()) {
                Object ret = response.response.getJavaObject();
                if(ret instanceof ByteBuffer) {
                    // CON-509: PluginSerializable objects will be wrapped
                    // within a ComplexTObject as BINARY data
                    ret = serializer.deserialize((ByteBuffer) ret);
                }
                if(RETURN_TRANSFORM.contains(method)) {
                    // Must transform the TObject(s) from the server into
                    // standard java objects to conform with the
                    // StatefulConcourseService interface.
                    if(ret instanceof TObjectResultDataset) {
                        ret = new ObjectResultDataset(
                                (TObjectResultDataset) ret);
                    }
                    else {
                        ret = Convert.possibleThriftToJava(ret);
                    }
                }
                return (T) ret;
            }
            else {
                throw Throwables.propagate(response.error);
            }
        }
        catch (ClassCastException e) {
            throw new RuntimeException("Illegal attempt to use "
                    + ConcourseRuntime.class.getSimpleName()
                    + " from an unsupported thread");
        }
    }

    /**
     * Singleton instance so that multiple plugins in the same distribution do
     * not unnecessarily create multiple runtime objects.
     */
    private static final ConcourseRuntime INSTANCE = init();

    /**
     * Construct a new instance.
     */
    @Inject
    protected ConcourseRuntime() {
        // NOTE: Routing to the correct Concourse Server instance is handled via
        // communication channels stored in each thread that accesses this
        // instance.
    }

    /**
     * An internal annotation used to denote that a method call should not be
     * proxied via the {@link ServerInvoker}.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public @interface NoGuice {}

    /**
     * An object that intercepts method invocations and proxies them to the
     * {@link ConcourseRuntime#invokeServer(String, Object...)} method.
     * 
     * @author Jeff Nelson
     */
    static class ServerInvoker implements MethodInterceptor {

        @Override
        public Object invoke(MethodInvocation invocation) throws Throwable {
            return invokeServer(invocation.getMethod().getName(),
                    invocation.getArguments());
        }

    }

    /**
     * A module that binds all methods not annotated with {@link NoGuice} to use
     * the {@link ServerInvoker} intercepter.
     * 
     * @author Jeff Nelson
     */
    static class ServerInvokerModule extends AbstractModule {

        @Override
        protected void configure() {
            bindInterceptor(Matchers.subclassesOf(ConcourseRuntime.class),
                    Matchers.not(Matchers.annotatedWith(NoGuice.class)),
                    new ServerInvoker());
        }

    }

}
