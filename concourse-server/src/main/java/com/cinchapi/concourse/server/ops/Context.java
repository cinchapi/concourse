/*
 * Copyright (c) 2013-2019 Cinchapi Inc.
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
package com.cinchapi.concourse.server.ops;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import com.cinchapi.ccl.Parser;
import com.cinchapi.common.collect.Association;
import com.cinchapi.common.describe.Empty;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.Language;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.server.ConcourseServer;
import com.cinchapi.concourse.thrift.JavaThriftBridge;
import com.cinchapi.concourse.thrift.TCriteria;
import com.cinchapi.concourse.thrift.TOrder;
import com.cinchapi.concourse.util.Parsers;
import com.google.common.base.CaseFormat;
import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * The {@link Context} provides information and all the relevant inputs
 * of a {@link ConcourseServer} operation.
 * 
 * <p>
 * In the vast majority of cases, an operation creates, reads, updates or
 * deletes (CRUD) data. In general, an operation can have any of the following
 * aspects:
 * <ul>
 * <li>The <strong>operation</strong> itself</li>
 * <li>A <strong>condition</strong> that specifies the data where the operation
 * should be applied</li>
 * <li>An <strong>order</strong> that describes how to sort data that is
 * returned as a side-effect of the operation</li>
 * <li>A <strong>page</strong> that describes how to window/limit data that is
 * returned as a side-effect of the operation</li>
 * </ul>
 * </p>
 * <p>
 * This class returns "relevant" data for the above-mentioned aspects.
 * </p>
 * <p>
 * {@link Context} is stored as a {@link ThreadLocal thread local} variable and
 * should be accessed from the operation's execution thread using
 * {@link #currentContext()}.
 * </p>
 *
 * @author Jeff Nelson
 */
@NotThreadSafe
public final class Context {

    /**
     * The {@link Context} that is returned from {@link #currentContext()}.
     * <p>
     * This value is held within a {@link ThreadLocal} so that each distinct
     * operation {@link Thread} sees a different reference. It is assumed that
     * the current context will be retrieved <strong>before</strong> any
     * subsequent asynchronous or multi-threaded processing.
     * </p>
     * <p>
     * A {@link Thread} can set this reference using the
     * {@link ThreadLocal#set(Object)} method.
     * </p>
     */
    /* package */static ThreadLocal<Context> current;

    /**
     * A collection of methods whose output should not be included in the
     * {@link #toString string} representation of this class.
     */
    private static Set<String> TO_STRING_BLACKLIST = Arrays
            .stream(Object.class.getDeclaredMethods())
            .filter(m -> m.getParameterCount() == 0).map(Method::getName)
            .collect(Collectors.toSet());

    /**
     * Return a reference to the current {@link Context}.
     * 
     * @return the current {@link Context}
     */
    @Nonnull
    public static Context currentContext() {
        Context ctx = current.get();
        if(ctx != null) {
            return ctx;
        }
        else {
            throw new IllegalStateException("The current context is null");
        }
    }

    /**
     * The keys upon which an operation is conducted.
     */
    private Set<String> operationKeys;

    /**
     * The records in which an operation is conducted.
     */
    private Set<Long> operationRecords;

    /**
     * The keys involved in a {@link Parser CCL condition} that determines the
     * {@link #operationRecords} that weren't explicitly provided.
     */
    private Set<String> conditionKeys;

    /**
     * The {@link Order} specified for sorting a result set.
     */
    @Nullable
    private Order order;

    /**
     * A {@link Parser} for any provided CCL condition.
     */
    @Nullable
    private Parser parser;

    /**
     * The type of operation that was requested.
     */
    private String operation;

    /**
     * The invoked operation {@link Method}.
     */
    private transient final Method method;

    /**
     * The params passed to the invoked {@link #method}.
     */
    private transient final Object[] params;

    /**
     * Tracks whether {@link #init()} has been called.
     */
    private boolean initialized = false;

    /**
     * Construct a new instance.
     * 
     * @param method
     * @param params
     */
    Context(Method method, Object... params) {
        this.method = method;
        this.params = params;
    }

    /**
     * Return he keys involved in a {@link Parser CCL condition} that determines
     * the {@link #operationRecords} that weren't explicitly provided.
     * 
     * @return any keys referenced in a {@link Criteria} or CCL condition
     */
    public Set<String> conditionKeys() {
        init();
        return conditionKeys;
    }

    /**
     * Return the type of operation that was requested. This is not the same as
     * the invoked server method, but rather the user-facing method that is
     * typically overloaded on the client side.
     * 
     * @return the operation name
     */
    public String operation() {
        init();
        return operation;
    }

    /**
     * Return the keys upon which an operation is conducted.
     * 
     * @return the keys that are affected by the operation
     */
    public Set<String> operationKeys() {
        init();
        return operationKeys;
    }

    /**
     * Return the records in which an operation is conducted.
     * 
     * @return the records that are affected by the operation
     */
    public Set<Long> operationRecords() {
        init();
        return operationRecords;
    }

    /**
     * Return the {@link Order} may have been included with the operation. If
     * none was provided, this method returns {@code null}.
     * 
     * @return the {@link Order} that specifies how to sort the result set
     */
    @Nullable
    public Order order() {
        init();
        return order;
    }

    /**
     * Return any keys referenced by the {@link #order()}.
     * 
     * @return the keys that determine the result set {@link Order}
     */
    public Set<String> orderKeys() {
        init();
        return order != null ? order.keys() : ImmutableSet.of();
    }

    /**
     * Return the {@link Parser} for any {@link Criteria} or {@link CCL}
     * condition that was included with the operation.
     * 
     * @return the {@link Parser} for the condition
     */
    @Nullable
    public Parser parser() {
        init();
        return parser;
    }

    @Override
    public String toString() {
        ToStringHelper generator = MoreObjects.toStringHelper(this);
        for (Method method : this.getClass().getDeclaredMethods()) {
            int mods = method.getModifiers();
            if(Modifier.isPublic(mods) && !Modifier.isStatic(mods)
                    && !TO_STRING_BLACKLIST.contains(method.getName())
                    && method.getParameterCount() == 0) {
                try {
                    Object value = method.invoke(this);
                    if(!Empty.ness().describes(value)) {
                        generator.add(method.getName(), value);
                    }
                }
                catch (ReflectiveOperationException e) {}
            }
        }
        return generator.toString();
    }

    /**
     * Inspect the operation and populate the context variables if they haven't
     * already been {@link #initialized}.
     */
    private void init() {
        if(!initialized) {
            String function = method.getName();
            String[] toks = CaseFormat.LOWER_CAMEL
                    .to(CaseFormat.LOWER_HYPHEN, function).split("-");
            Association args = Association.of();
            if(function.equals("getServerVersion")) {
                operation = function;
            }
            else if(function.equals("verifyOrSet")) {
                operation = function;
                args.put("key", params[0]);
                args.put("value", params[1]);
                args.put("record", params[2]);
            }
            else if(function.equals("verifyAndSwap")) {
                operation = function;
                args.put("key", params[0]);
                args.put("record", params[2]);
                args.put("values", ImmutableList.of(params[1], params[3]));
            }
            else if(function.equals("findOrInsertCclJson")) {
                operation = "findOrInsert";
                args.put("ccl", params[0]);
            }
            else if(function.equals("findOrInsertCriteriaJson")) {
                operation = "findOrInsert";
                args.put("criteria", params[0]);
            }
            else if(function.equals("findOrAddKeyValue")) {
                operation = "findOrAdd";
                args.put("key", params[0]);
                conditionKeys = ImmutableSet.of((String) params[0]);
            }
            else {
                operation = toks[0];
                for (int i = 1; i < toks.length; ++i) {
                    args.put(toks[i].toLowerCase(), params[i - 1]);
                }
            }
            /*
             * The convention is that ConcourseServer methods are named in the
             * form of "actionParam1Param2...ParamN". Param names matter. For
             * example, the param "Ccl" is assumed to refer to a String whereas
             * Criteria is assumed to refers to a TCriteria object. If the param
             * name ends in "s", it is assumed to refer to a List of the
             * underlying type (i.e. Keys is assumed to refer to a List<String>)
             * 
             * Key -> String
             * Value -> TObject
             * Record -> Long
             * Ccl -> String
             * Time -> Timestamp
             * Timestr -> String
             * Criteria -> TCriteria
             * Order -> TOrder
             * Page -> TPage
             * Start -> Timestamp
             * End -> Timstamp
             * Startstr -> String
             * Endstr -> String
             */
            // TODO: what if there is local data for the parser;
            // Parser
            if(args.containsKey("ccl")) {
                String ccl = args.fetch("ccl");
                parser = Parsers.create(ccl);
            }
            else if(args.containsKey("criteria")) {
                TCriteria tcriteria = args.fetch("criteria");
                Criteria criteria = Language
                        .translateFromThriftCriteria(tcriteria);
                parser = Parsers.create(criteria);
            }
            else {
                parser = null;
            }

            // operationKeys
            if(args.containsKey("key")) {
                String key = args.fetch("key");
                operationKeys = Sets.newHashSet(key);
            }
            else if(args.containsKey("keys")) {
                Collection<String> keys = args.fetch("keys");
                operationKeys = Sets.newHashSet(keys);
            }
            else {
                operationKeys = ImmutableSet.of();
            }

            // operationRecords
            if(args.containsKey("record")) {
                long record = args.fetch("record");
                operationRecords = Sets.newHashSet(record);
            }
            else if(args.containsKey("records")) {
                Collection<Long> records = args.fetch("records");
                operationRecords = Sets.newHashSet(records);
            }
            else {
                operationRecords = ImmutableSet.of();
            }

            // order
            if(args.containsKey("order")) {
                TOrder torder = args.fetch("order");
                order = JavaThriftBridge.convert(torder);
            }
            else {
                order = null;
            }

            // NOTE: #conditionKeys can be set manually for some special case
            // methods above.
            if(conditionKeys == null && parser != null) {
                conditionKeys = parser.analyze().keys();
            }
            else if(function.startsWith("findKey")) {
                conditionKeys = ImmutableSet.of((String) params[0]);
            }
            else if(conditionKeys == null) {
                conditionKeys = ImmutableSet.of();
            }

        }
    }

}
