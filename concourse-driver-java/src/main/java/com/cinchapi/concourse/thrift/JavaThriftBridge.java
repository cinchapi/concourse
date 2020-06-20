/*
 * Copyright (c) 2013-2020 Cinchapi Inc.
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
package com.cinchapi.concourse.thrift;

import java.util.List;
import java.util.stream.Collectors;

import com.cinchapi.ccl.grammar.FunctionTokenSymbol;
import com.cinchapi.ccl.syntax.FunctionTree;
import com.cinchapi.ccl.type.function.ExplicitBinaryFunction;
import com.cinchapi.ccl.util.NaturalLanguage;
import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.lang.ConcourseCompiler;
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.Direction;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.lang.sort.OrderComponent;
import com.cinchapi.concourse.util.Convert;

/**
 * A utility class for converting/translating/etc between Java and Thrift types.
 *
 * @author Jeff Nelson
 */
public final class JavaThriftBridge {

    // NOTE: Eventually, this class should replace Convert#thriftToJava, Convert
    // ##javaToThrift and Language#translate*

    /**
     * Translate an {@link Order} to a {@link TOrder}.
     * 
     * @param order
     * @return the analogous {@link TOrder}
     */
    public static TOrder convert(Order order) {
        List<TOrderComponent> tcomponents = order.spec().stream()
                .map(JavaThriftBridge::convert).collect(Collectors.toList());
        TOrder torder = new TOrder(tcomponents);
        return torder;
    }

    /**
     * Translate an {@link OrderComponent} to a {@link TOrderComponent}.
     * 
     * @param component
     * @return the analogous {@link TOrderComponent}
     */
    public static TOrderComponent convert(OrderComponent component) {
        TOrderComponent tcomponent = new TOrderComponent(component.key(),
                component.direction().coefficient());
        if(component.timestamp() != null) {
            Object timestamp;
            try {
                timestamp = component.timestamp().getMicros();
            }
            catch (IllegalStateException e) {
                timestamp = component.timestamp().toString();
            }
            tcomponent.setTimestamp(Convert.javaToThrift(timestamp));
        }
        return tcomponent;
    }

    /**
     * Translate a {@link Page} to a {@link TPage}.
     * 
     * @param page
     * @return the analogous {@link TPage}
     */
    public static TPage convert(Page page) {
        return new TPage(page.skip(), page.limit());
    }

    /**
     * Translate a {@link TOrder} to an {@link Order}.
     * 
     * @param torder
     * @return the analogous {@link Order}
     */
    public static Order convert(TOrder torder) {
        List<OrderComponent> components = torder.getSpec().stream()
                .map(JavaThriftBridge::convert).collect(Collectors.toList());
        if(components.isEmpty()) {
            return Order.none();
        }
        else {
            return new Order() {

                @Override
                public List<OrderComponent> spec() {
                    return components;
                }

            };
        }
    }

    @SuppressWarnings("unchecked")
    public static <S> ExplicitBinaryFunction<S> tryParseAsFunction(
            TObject tobject) {
        if(tobject.getType() == Type.STRING) {
            String object = (String) Convert.thriftToJava(tobject);
            char[] chars = object.toCharArray();
            if(chars[chars.length - 1] == ')') {
                try {
                    FunctionTree tree = (FunctionTree) ConcourseCompiler.get()
                            .parse(object);
                    FunctionTokenSymbol symbol = (FunctionTokenSymbol) tree
                            .root();
                    return (ExplicitBinaryFunction<S>) symbol.function();
                }
                catch (Exception e) {
                    /* ignore and assume that the TObject isn't a function */
                }
            }
        }
        return null;
    }

    /**
     * Return a {@link TOrderComponent} to an {@link OrderComponent}.
     * 
     * @param tcomponent
     * @return the analogous {@link OrderComponent}
     */
    public static OrderComponent convert(TOrderComponent tcomponent) {
        Object $timestamp = tcomponent.isSetTimestamp()
                ? Convert.thriftToJava(tcomponent.getTimestamp()) : null;
        if($timestamp != null && !($timestamp instanceof Number)) {
            // Assume that this method is being called from ConcourseServer and
            // convert a string timestamp to micros so that it can be properly
            // handled in the Sorter.
            try {
                $timestamp = NaturalLanguage.parseMicros($timestamp.toString());
            }
            catch (Exception e) {/* ignore */}
        }
        Timestamp timestamp = $timestamp != null ? ($timestamp instanceof Number
                ? Timestamp.fromMicros(((Number) $timestamp).longValue())
                : Timestamp.fromString($timestamp.toString())) : null;
        Direction direction = null;
        for (Direction $direction : Direction.values()) {
            if(tcomponent.getDirection() == $direction.coefficient()) {
                direction = $direction;
                break;
            }
        }
        return new OrderComponent(tcomponent.getKey(), timestamp, direction);
    }

    /**
     * Translate a {@link TPage} to a {@link Page}.
     * 
     * @param page
     * @return the analogous {@link Page}
     */
    public static Page convert(TPage tpage) {
        return Page.of(tpage.getSkip(), tpage.getLimit());
    }

    private JavaThriftBridge() {/* no-init */}

}
