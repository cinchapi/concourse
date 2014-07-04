/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2014 Jeff Nelson, Cinchapi Software Collective
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.cinchapi.concourse.server.storage;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.cinchapi.concourse.server.model.Value;
import org.cinchapi.concourse.server.storage.temp.Write;
import org.cinchapi.concourse.testing.Variables;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.util.Convert;
import org.cinchapi.concourse.util.Numbers;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.theories.Theory;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;

/**
 * Unit tests for {@link BufferedStore} that try to stress scenarios that occur
 * when data offsetting data is split between the destination and buffer.
 * 
 * @author jnelson
 */
public abstract class BufferedStoreTest extends StoreTest {
    // NOTE: All tests names should use "Buffered" in the name so they do not
    // override or conflict with tests in {@link StoreTest}.

    // Tests are randomized, but we need a controlled list of data components to
    // reliably generate correctly offsetting data. It is okay to add more
    // components to these lists, but the number of POSSIBLE_KEYS should always
    // be less than or equal to the size of the other lists.
    private static final List<String> POSSIBLE_KEYS = Lists.newArrayList("A",
            "B", "C", "D");
    private static final List<TObject> POSSIBLE_VALUES = Lists.newArrayList(
            Convert.javaToThrift("one"), Convert.javaToThrift("two"),
            Convert.javaToThrift("three"), Convert.javaToThrift("four"),
            Convert.javaToThrift("five"), Convert.javaToThrift("six"),
            Convert.javaToThrift("seven"), Convert.javaToThrift("eight"),
            Convert.javaToThrift("nine"), Convert.javaToThrift("ten"));
    private static final List<Long> POSSIBLE_RECORDS = Lists.newArrayList(1L,
            2L, 3L, 4L, 5L, 6L, 7L);
    
//    @Test
//    public void testAuditRecordBuffered(){
////        List<Data> data = generateTestData();
////        insertData(data);
////        Data d = data.get(TestData.getScaleCount() % data.size());
//        //TODO finish
//    }
//    
//    @Test
//    public void testAuditKeyInRecordBuffered(){
//        //TODO
//    }
    
    /**
     * Convert the data elements to a {@link Table}.
     * @param data
     * @return a Table with the data
     */
    @SuppressWarnings("unused")
    private Table<Long, String, Set<TObject>> convertDataToTable(List<Data> data){
        Table<Long, String, Set<TObject>> table = HashBasedTable.create();
        Iterator<Data> it = data.iterator();
        while(it.hasNext()){
            Data x = it.next();
            Set<TObject> values = table.get(x.record, x.key);
            if(values == null){
                values = Sets.newHashSet();
                table.put(x.record, x.key, values);
            }
            if(x.type == Action.ADD){
                values.add(x.value);
            }
            else{
                values.remove(x.value);
            }        
        }
        return table;
    }

    @Test
    public void testDescribeBuffered() {
        List<Data> data = generateTestData();
        insertData(data);
        Data d = data.get(TestData.getScaleCount() % data.size());
        Map<String, Set<TObject>> ktv = Maps.newHashMap();
        Iterator<Data> it = data.iterator();
        while (it.hasNext()) {
            Data _d = it.next();
            if(_d.record == d.record) {
                Set<TObject> values = ktv.get(_d.key);
                if(values == null) {
                    values = Sets.newHashSet();
                    ktv.put(_d.key, values);
                }
                if(_d.type == Action.ADD) {
                    Assert.assertTrue(values.add(_d.value));
                }
                else {
                    Assert.assertTrue(values.remove(_d.value));
                }
            }
        }
        Set<String> keys = Sets.newHashSet();
        Iterator<String> it2 = ktv.keySet().iterator();
        while (it2.hasNext()) {
            String key = it2.next();
            if(!ktv.get(key).isEmpty()) {
                keys.add(key);
            }
        }
        Assert.assertEquals(keys, store.describe(d.record));
    }

    @Test
    public void testFetchBuffered() {
        List<Data> data = generateTestData();
        insertData(data);
        Data d = Variables.register("d",
                data.get(TestData.getScaleCount() % data.size()));
        Set<TObject> values = Sets.newHashSet();
        Iterator<Data> it = data.iterator();
        while (it.hasNext()) {
            Data _d = it.next();
            if(_d.record == d.record && _d.key.equals(d.key)) {
                if(_d.type == Action.ADD) {
                    Assert.assertTrue(values.add(_d.value));
                }
                else {
                    Assert.assertTrue(values.remove(_d.value));
                }
            }
        }
        Assert.assertEquals(values, store.fetch(d.key, d.record));
    }
   

    @Test
    @Theory
    public void testFindBuffered(Operator operator) {
        List<Data> data = generateTestData();
        insertData(data);
        Data d = data.get(TestData.getScaleCount() % data.size());
        Variables.register("operator", operator);
        doTestFindBuffered(data, d, operator);
    }

    @Test
    public void testVerifyBuffered() {
        List<Data> data = generateTestData();
        insertData(data);
        Data d = Variables.register("d",
                data.get(TestData.getScaleCount() % data.size()));
        boolean verify = Numbers.isOdd(count(data, d));
        Assert.assertEquals(verify, store.verify(d.key, d.value, d.record));
    }

    /**
     * Count the number of times that {@code element} appears in the list of
     * {@code data}. If the result is even, then {@code element} is net neutral,
     * otherwise is is net positive.
     * 
     * @param data
     * @param element
     * @return the count for {@code element}
     */
    private int count(List<Data> data, Data element) {
        int i = 0;
        for (Data d : data) {
            i += d.equals(element) ? 1 : 0;
        }
        return i;
    }

    /**
     * Execute the findBuffered test.
     * 
     * @param data
     * @param d
     * @param operator
     */
    private void doTestFindBuffered(List<Data> data, Data d, Operator operator) {
        Variables.register("d", d);
        Variables.register("operator", operator);
        Map<Long, Set<TObject>> rtv = Maps.newHashMap();
        Iterator<Data> it = data.iterator();
        while (it.hasNext()) {
            Data _d = it.next();
            if(_d.key.equals(d.key)) {
                // NOTE: It is necessaty to wrap the TObjects as Values because
                // TObject compareTo is not correctly defined.
                Value v1 = Value.wrap(d.value);
                Value v2 = Value.wrap(_d.value);
                boolean matches = false;
                if(operator == Operator.EQUALS) {
                    matches = v1.equals(v2);
                }
                else if(operator == Operator.NOT_EQUALS) {
                    matches = !v1.equals(v2);
                }
                else if(operator == Operator.GREATER_THAN) {
                    matches = v1.compareTo(v2) < 0;
                }
                else if(operator == Operator.GREATER_THAN_OR_EQUALS) {
                    matches = v1.compareTo(v2) <= 0;
                }
                else if(operator == Operator.LESS_THAN) {
                    matches = v1.compareTo(v2) > 0;
                }
                else if(operator == Operator.LESS_THAN_OR_EQUALS) {
                    matches = v1.compareTo(v2) >= 0;
                }
                else if(operator == Operator.BETWEEN) {
                    // TODO Implement this later. We will need to get a a second
                    // value from the list of data
                }
                else if(operator == Operator.REGEX) {
                    matches = v2.toString().matches(v1.toString());
                }
                else if(operator == Operator.NOT_REGEX) {
                    matches = !v2.toString().matches(v1.toString());
                }
                else {
                    throw new UnsupportedOperationException();
                }
                if(matches) {
                    Set<TObject> values = rtv.get(_d.record);
                    if(values == null) {
                        values = Sets.newHashSet();
                        rtv.put(_d.record, values);
                    }
                    if(_d.type == Action.ADD) {
                        Assert.assertTrue(values.add(_d.value));
                    }
                    else {
                        Assert.assertTrue(values.remove(_d.value));
                    }
                }

            }
        }
        Set<Long> records = Sets.newHashSet();
        Iterator<Long> it2 = rtv.keySet().iterator();
        while (it2.hasNext()) {
            long record = it2.next();
            if(!rtv.get(record).isEmpty()) {
                records.add(record);
            }
        }
        Assert.assertEquals(records, store.find(d.key, operator, d.value));
    }

    /**
     * <p>
     * Return a random sequence of Data that should be added to the
     * BufferedStore in order. The ordered sequence simulates data being added
     * and removed for a controlled list of keys, values and records. The
     * returned list will either be net neutral (meaning all positive data is
     * offset by negative data) or it will be net positive (meaning there is
     * some positive data that is not offset by negative data). <strong>It
     * should never be net negative</strong>
     * </p>
     * <p>
     * The caller can determine how many times a piece of data appears in the
     * list (and therefore whether it is net neutral (even number) or net
     * positive (odd number) using the {@link #count(List, Data)}.
     * </p>
     * 
     * @return the data
     */
    private List<Data> generateTestData() {
        // Setup iterators
        Iterator<String> keys = Iterators.cycle(POSSIBLE_KEYS);
        Iterator<TObject> values = Iterators.cycle(POSSIBLE_VALUES);
        Iterator<Long> records = Iterators.cycle(POSSIBLE_RECORDS);

        // Get initial positive and negative data so we can guarantee that every
        // remove offsets an add
        int numNegData = TestData.getScaleCount();
        List<Data> posData = Lists.newArrayList();
        List<Data> negData = Lists.newArrayList();
        for (int i = 0; i < numNegData; i++) {
            Data pos = Data
                    .positive(keys.next(), values.next(), records.next());
            Data neg = Data.negative(pos);
            posData.add(pos);
            negData.add(neg);
        }

        // Get more positive data, no greater than the number of available keys
        // (the smallest list) so that we can guarantee that we don't have any
        // adds that aren't offset
        int numAddlPosData = TestData.getScaleCount() % POSSIBLE_KEYS.size();
        for (int i = 0; i < numAddlPosData; i++) {
            Data pos = Data
                    .positive(keys.next(), values.next(), records.next());
            posData.add(pos);
        }

        // Create the order in which the data will be written
        List<Data> order = Lists.newArrayList();
        boolean lastWasNeg = true;
        while (posData.size() > 0 || negData.size() > 0) {
            if(lastWasNeg && posData.size() > 0) {
                int index = TestData.getScaleCount() % posData.size();
                if(Numbers.isEven(count(order, posData.get(index)))) {
                    order.add(posData.get(index));
                    posData.remove(index);
                }
                lastWasNeg = false;
            }
            else {
                if(negData.size() > 0) {
                    int index = TestData.getScaleCount() % negData.size();
                    if(Numbers.isOdd(count(order, negData.get(index)))) {
                        order.add(negData.get(index));
                        negData.remove(index);
                    }
                    lastWasNeg = true;
                }
            }
        }
        return Variables.register("order", order);
    }

    /**
     * Insert {@code data} into the BufferedStore, randomly splitting it between
     * the destination and buffer. To control the amount that goes into the
     * destination, use the {@link #insertData(List, int)} method.
     * 
     * @param data
     */
    private void insertData(List<Data> data) {
        insertData(data, TestData.getScaleCount() % data.size());
    }

    /**
     * Insert the first {@code numForDestination} elements from {@code data}
     * into the destination and the rest into the buffer.
     * 
     * @param data
     * @param numForDestination
     */
    private void insertData(List<Data> data, int numForDestination) {
        Preconditions.checkArgument(numForDestination <= data.size());
        Variables.register("numForDestination", numForDestination);
        Iterator<Data> it = data.iterator();
        for (int i = 0; i < numForDestination; i++) {
            Data d = it.next();
            if(d.type == Action.ADD) {
                ((BufferedStore) store).destination.accept(Write.add(d.key,
                        d.value, d.record));
            }
            else {
                ((BufferedStore) store).destination.accept(Write.remove(d.key,
                        d.value, d.record));
            }
        }
        while (it.hasNext()) {
            Data d = it.next();
            if(d.type == Action.ADD) {
                ((BufferedStore) store).buffer.insert(Write.add(d.key, d.value,
                        d.record));
            }
            else {
                ((BufferedStore) store).buffer.insert(Write.remove(d.key,
                        d.value, d.record));
            }
        }
    }

    /**
     * Return the sequence of data recovered from parsing the toString output
     * from the list. This method should be used recreate test conditions of
     * unit test failures.
     * 
     * @param orderString
     * @return
     */
    @SuppressWarnings("unused")
    private List<Data> recoverTestData(String orderString) {
        orderString = orderString.replaceAll("\\]", "").replaceAll("\\[", "");
        String[] toks = orderString.split(",");
        List<Data> data = Lists.newArrayList();
        for (String tok : toks) {
            data.add(Data.fromString(tok));
        }
        Variables.register("order", data);
        return data;
    }

    /**
     * A test class that encapsulates data that is added/removed to/from the
     * BufferedStore.
     * 
     * @author jnelson
     */
    private static final class Data {

        /**
         * Return the Data element that is described by {@code string}
         * 
         * @param string
         * @return the data
         */
        public static Data fromString(String string) {
            string = string.trim();
            String[] toks = string.split(" ");
            return new Data(Action.valueOf(toks[0]), toks[1],
                    Convert.javaToThrift(toks[3]), Long.valueOf(toks[5]));
        }

        /**
         * Return a negative Data element that is an offset of {@code data}.
         * 
         * @param data
         * @return the negative offset for {@code data}
         */
        public static Data negative(Data data) {
            return new Data(Action.REMOVE, data.key, data.value, data.record);

        }

        /**
         * Return a positive Data element.
         * 
         * @param key
         * @param value
         * @param record
         * @return the data
         */
        public static Data positive(String key, TObject value, long record) {
            return new Data(Action.ADD, key, value, record);
        }

        private final long record;
        private final String key;
        private final TObject value;
        private final Action type;

        /**
         * Construct a new instance.
         * 
         * @param type
         * @param key
         * @param value
         * @param record
         */
        private Data(Action type, String key, TObject value, long record) {
            this.type = type;
            this.key = key;
            this.value = value;
            this.record = record;
        }

        @Override
        public boolean equals(Object obj) {
            if(obj instanceof Data) {
                return ((Data) obj).key.equals(key)
                        && ((Data) obj).value.equals(value)
                        && ((Data) obj).record == record;
            }
            return false;
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, value, record);
        }

        @Override
        public String toString() {
            return type + " " + key + " AS " + value + " IN " + record;
        }

    }

}
