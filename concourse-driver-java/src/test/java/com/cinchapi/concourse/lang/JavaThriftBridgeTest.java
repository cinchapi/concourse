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
package com.cinchapi.concourse.lang;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.lang.sort.Direction;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.lang.sort.OrderComponent;
import com.cinchapi.concourse.thrift.JavaThriftBridge;
import com.cinchapi.concourse.thrift.TOrder;
import com.cinchapi.concourse.thrift.TOrderComponent;
import com.cinchapi.concourse.thrift.Type;

/**
 * Unit tests for {@link Language} utility class.
 *
 * @author Jeff Nelson
 */
public class JavaThriftBridgeTest {

    @Test
    public void testOrderConvertRoundTrip() {
        Order expected = Order.by("a").then("b").descending().then().by("c")
                .at(Timestamp.now()).then("d");
        TOrder torder = JavaThriftBridge.convert(expected);
        Order actual = JavaThriftBridge.convert(torder);
        Assert.assertEquals(expected.spec(), actual.spec());
    }

    @Test
    public void testOrderComponentWithStringTimestampConvert() {
        OrderComponent component = new OrderComponent("a",
                Timestamp.fromString("yesterday"), Direction.ASCENDING);
        TOrderComponent tcomponent = JavaThriftBridge.convert(component);
        Assert.assertTrue(tcomponent.getTimestamp().getType() == Type.STRING);
    }

}
