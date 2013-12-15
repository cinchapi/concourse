/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013 Jeff Nelson, Cinchapi Software Collective
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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import org.cinchapi.concourse.ConcourseBaseTest;
import org.cinchapi.concourse.testing.Variables;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.Convert;
import org.cinchapi.concourse.util.Numbers;
import org.cinchapi.concourse.util.TStrings;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Base unit tests for {@link Store} services.
 * 
 * @author jnelson
 */
@RunWith(Theories.class)
public abstract class StoreTest extends ConcourseBaseTest {

    public final Logger log = LoggerFactory.getLogger(getClass());

    @DataPoints
    public static Operator[] operators = { Operator.EQUALS,
            Operator.GREATER_THAN, Operator.GREATER_THAN_OR_EQUALS,
            Operator.LESS_THAN, Operator.LESS_THAN_OR_EQUALS,
            Operator.NOT_EQUALS };

    @DataPoints
    public static SearchType[] searchTypes = { SearchType.PREFIX,
            SearchType.INFIX, SearchType.SUFFIX, SearchType.FULL };

    protected Store store;

    @Rule
    public TestRule watcher = new TestWatcher() {

        @Override
        protected void finished(Description desc) {
            cleanup(store);
        }

        @Override
        protected void starting(Description desc) {
            store = getStore();
            store.start();
        }
    };

    // TODO test audit

    @Test
    public void testDescribeAfterAddAndRemoveSingle() {
        String key = TestData.getString();
        TObject value = TestData.getTObject();
        long record = TestData.getLong();
        add(key, value, record);
        remove(key, value, record);
        Assert.assertFalse(store.describe(record).contains(key));
    }

    @Test
    public void testDescribeAfterAddAndRemoveSingleWithTime() {
        String key = TestData.getString();
        TObject value = TestData.getTObject();
        long record = TestData.getLong();
        add(key, value, record);
        long timestamp = Time.now();
        remove(key, value, record);
        Assert.assertTrue(store.describe(record, timestamp).contains(key));
    }

    @Test
    public void testDescribeAfterAddMulti() {
        long record = TestData.getLong();
        Set<String> keys = getKeys();
        for (String key : keys) {
            add(key, TestData.getTObject(), record);
        }
        Assert.assertEquals(keys, store.describe(record));
    }

    @Test
    public void testDescribeAfterAddMultiAndRemoveMulti() {
        long record = TestData.getLong();
        Set<String> keys = getKeys();
        int count = 0;
        for (String key : keys) {
            add(key, Convert.javaToThrift(count), record);
            count++;
        }
        Iterator<String> it = keys.iterator();
        count = 0;
        while (it.hasNext()) {
            String key = it.next();
            if(TestData.getInt() % 3 == 0) {
                it.remove();
                remove(key, Convert.javaToThrift(count), record);
            }
            count++;
        }
        Assert.assertEquals(keys, store.describe(record));
    }

    @Test
    public void testDescribeAfterAddMultiAndRemoveMultiWithTime() {
        long record = TestData.getLong();
        Set<String> keys = getKeys();
        int count = 0;
        for (String key : keys) {
            add(key, Convert.javaToThrift(count), record);
            count++;
        }
        Iterator<String> it = keys.iterator();
        count = 0;
        while (it.hasNext()) {
            String key = it.next();
            if(TestData.getInt() % 3 == 0) {
                it.remove();
                remove(key, Convert.javaToThrift(count), record);
            }
            count++;
        }
        long timestamp = Time.now();
        count = 0;
        for (String key : getKeys()) {
            add(key, Convert.javaToThrift(count), record);
            count++;
        }
        Assert.assertEquals(keys, store.describe(record, timestamp));
    }

    @Test
    public void testDescribeAfterAddMultiWithTime() {
        long record = TestData.getLong();
        Set<String> keys = getKeys();
        for (String key : keys) {
            add(key, TestData.getTObject(), record);
        }
        long timestamp = Time.now();
        for (String key : getKeys()) {
            add(key, TestData.getTObject(), record);
        }
        Assert.assertEquals(keys, store.describe(record, timestamp));
    }

    @Test
    public void testDescribeAfterAddSingle() {
        String key = TestData.getString();
        long record = TestData.getLong();
        add(key, TestData.getTObject(), record);
        Assert.assertTrue(store.describe(record).contains(key));
    }

    @Test
    public void testDescribeAfterAddSingleWithTime() {
        String key = TestData.getString();
        long record = TestData.getLong();
        long timestamp = Time.now();
        add(key, TestData.getTObject(), record);
        Assert.assertFalse(store.describe(record, timestamp).contains(key));
    }

    @Test
    public void testDescribeEmpty() {
        Assert.assertTrue(store.describe(TestData.getLong()).isEmpty());
    }

    @Test
    public void testFetchAfterAddAndRemoveSingle() {
        String key = TestData.getString();
        TObject value = TestData.getTObject();
        long record = TestData.getLong();
        add(key, value, record);
        remove(key, value, record);
        Assert.assertFalse(store.fetch(key, record).contains(value));
    }

    @Test
    public void testFetchAfterAddAndRemoveSingleWithTime() {
        String key = TestData.getString();
        TObject value = TestData.getTObject();
        long record = TestData.getLong();
        add(key, value, record);
        long timestamp = Time.now();
        remove(key, value, record);
        Assert.assertTrue(store.fetch(key, record, timestamp).contains(value));
    }

    @Test
    public void testFetchAfterAddMulti() {
        String key = TestData.getString();
        long record = TestData.getLong();
        Set<TObject> values = getValues();
        for (TObject value : values) {
            add(key, value, record);
        }
        Assert.assertEquals(values, store.fetch(key, record));
    }

    @Test
    public void testFetchAfterAddMultiAndRemoveMulti() {
        String key = TestData.getString();
        long record = TestData.getLong();
        Set<TObject> values = getValues();
        for (TObject value : values) {
            add(key, value, record);
        }
        Iterator<TObject> it = values.iterator();
        while (it.hasNext()) {
            TObject value = it.next();
            if(TestData.getInt() % 3 == 0) {
                it.remove();
                remove(key, value, record);
            }
        }
        Assert.assertEquals(values, store.fetch(key, record));
    }

    @Test
    public void testFetchAfterAddMultiAndRemoveMultiWithTime() {
        String key = TestData.getString();
        long record = TestData.getLong();
        Set<TObject> values = getValues();
        for (TObject value : values) {
            add(key, value, record);
        }
        Iterator<TObject> it = values.iterator();
        while (it.hasNext()) {
            TObject value = it.next();
            if(TestData.getInt() % 3 == 0) {
                it.remove();
                remove(key, value, record);
            }
        }
        long timestamp = Time.now();
        List<TObject> otherValues = Lists.newArrayList();
        for (TObject value : getValues()) {
            while (values.contains(value)) {
                value = TestData.getTObject();
            }
            add(key, value, record);
            otherValues.add(value);
        }
        Set<TObject> valuesCopy = Sets.newHashSet(values);
        for (TObject value : getValues()) {
            if(valuesCopy.contains(value) || otherValues.contains(value)) {
                remove(key, value, record);
                otherValues.remove(value);
                valuesCopy.remove(value);
            }
        }
        Assert.assertEquals(values, store.fetch(key, record, timestamp));
    }

    @Test
    public void testFetchAfterAddMultiWithTime() {
        String key = TestData.getString();
        long record = TestData.getLong();
        Set<TObject> values = getValues();
        for (TObject value : values) {
            add(key, value, record);
        }
        long timestamp = Time.now();
        for (TObject value : getValues()) {
            while (values.contains(value)) {
                value = TestData.getTObject();
            }
            add(key, value, record);
        }
        Assert.assertEquals(values, store.fetch(key, record, timestamp));
    }

    @Test
    public void testFetchAfterAddSingle() {
        String key = TestData.getString();
        TObject value = TestData.getTObject();
        long record = TestData.getLong();
        add(key, value, record);
        Assert.assertTrue(store.fetch(key, record).contains(value));
    }

    @Test
    public void testFetchAfterAddSingleWithTime() {
        String key = TestData.getString();
        TObject value = TestData.getTObject();
        long record = TestData.getLong();
        long timestamp = Time.now();
        add(key, value, record);
        Assert.assertFalse(store.fetch(key, record, timestamp).contains(value));
    }

    @Test
    public void testFetchEmpty() {
        Assert.assertTrue(store.fetch(TestData.getString(), TestData.getLong())
                .isEmpty());
    }

    @Test
    @Theory
    public void testFind(Operator operator) {
        String key = TestData.getString();
        Number min = TestData.getNumber();
        Set<Long> records = addRecords(key, min, operator);
        Assert.assertEquals(records,
                store.find(key, operator, Convert.javaToThrift(min)));
    }

    @Test
    @Theory
    public void testFindAfterRemove(Operator operator) {
        String key = TestData.getString();
        Number min = TestData.getNumber();
        Set<Long> records = removeRecords(key, addRecords(key, min, operator));
        Assert.assertEquals(records,
                store.find(key, operator, Convert.javaToThrift(min)));
    }

    @Test
    @Theory
    public void testFindAfterRemoveWithTime(Operator operator) {
        String key = TestData.getString();
        Number min = TestData.getNumber();
        Set<Long> records = removeRecords(key, addRecords(key, min, operator));
        long timestamp = Time.now();
        removeRecords(key, addRecords(key, min, operator));
        Assert.assertEquals(records,
                store.find(timestamp, key, operator, Convert.javaToThrift(min)));
    }

    @Test
    @Theory
    public void testFindWithTime(Operator operator) {
        String key = TestData.getString();
        Number min = TestData.getNumber();
        Set<Long> records = addRecords(key, min, operator);
        long timestamp = Time.now();
        addRecords(key, min, operator);
        Assert.assertEquals(records,
                store.find(timestamp, key, operator, Convert.javaToThrift(min)));
    }

    @Test
    @Theory
    public void testSearch(SearchType type) {
        String query = null;
        while (query == null) {
            query = TestData.getString();
        }
        Variables.register("query", query);
        String key = Variables.register("key", TestData.getString());
        Set<Long> records = setupSearchTest(key, query, type);
        Assert.assertEquals(records, store.search(key, query));
    }

    @Test
    @Theory
    public void testSeachReproA(SearchType type) {
        String query = Variables
                .register(
                        "query",
                        "i7rwvli7esvitzio2 qp  arxwlclruja ulzfhtl4yyxopsc  bk57q2tz30 0y606dwynvffp6vqx");
        String key = Variables.register("key", "foo");
        Set<Long> records = setupSearchTest(
                key,
                query,
                type,
                Lists.newArrayList(1L),
                Lists.newArrayList("1ub0gsi61bz39y90wbe96rvxo3g4mtt89sg1tfjsf4vuyyjc9oivc7sluuxrmj897ni15p8obu6i7rwvli7esvitzio2 qp  arxwlclruja ulzfhtl4yyxopsc  bk57q2tz30 0y606dwynvffp6vqx"));
        Assert.assertEquals(records, store.search(key, query));
    }

    @Test
    @Theory
    public void testSearchReproCON_7(SearchType type) {
        String query = Variables.register("query", "5");
        String key = Variables.register("key", "vhncr15x4vi1r7dx3bw8sgo3");
        Set<Long> records = setupSearchTest(
                key,
                query,
                type,
                Lists.newArrayList(4407787408868251656L, -81405110977674943L,
                        -7140839054266835785L, -2478927665696010310L,
                        218763128369680085L, 3303203363767514564L,
                        -5345452149459798319L, -2878094606020740280L,
                        -2364262306786179717L, -7073499293604063929L,
                        -1181274448039222311L, 74405258499052471L,
                        -3199762627894970848L, -1589943203396786380L,
                        3917764129096184859L, 5340961473029303427L,
                        -6765165614843560497L, -7193164394167202080L,
                        -2953063992651183477L, 2015523299602625665L,
                        8352547665716061424L),
                Lists.newArrayList(
                        "x6ovyg2524ez1n",
                        "x7 e0o5ouxxbs8 ykcj248ss873ds94x71eli1 7mvo963e9wnhc k36dek6t0hde8h6tc2bji436jislz2o 497nvdzn0ugr e",
                        "yn6isk627adyvm0j7k1l jm2xffyyciluvhrzu xrpn6607o pfyq1 3biam5b7odvnt4",
                        "0m0du5135vcnlmvv 924 e7ao0enajqri",
                        "kdqrlcg9b857na3 pf9cb4n4gqkuk1gza7z8gst",
                        "g yk9juwviyrts8pb6pwmu9inwue27y86ugqe5u2lloo2 o3t7bknpzb7705tdkjso9v5d5mz42l0z81zga2wjbvdb21ld2o9gey",
                        "qz7v 974khsb24bfukqehsfqpkgosifd   4t3 xoas0fp",
                        "xng2awwtylp5l6jsza1hyqg90zinagt p 2bzbawvx0 k0phvb5q o3fd",
                        "jyqy8cybzu7jxrhqxskxwsho8db9pan5a1gzuejssdluy5ja748nk0n0ii7ceq13n3ytd5",
                        "skvu5 i0a9i0mj3xrx7hnkjydlhh70uuautvw5qmkmhlzgda vtj42hr0 c02ahksg3xip6fo9n7 eaa3yo9gu",
                        "im2cseewi75t6ky45xcetc0hx6vpbr3p3vosgkre0j1pbtw8t550dbq71qt5",
                        "ofcak2so66bsljj1ne7c fmu2rns4aqcq7tk987yus3s7",
                        "mand ww0pn6b2q8q3qpzajuay0  ma   w34s59",
                        "hxjdt37fs73h7vpx2xods2lcd55ok4vc1egqbww7eup8qfajchjoalfjau4syzd6m8s5utqpcidyfw",
                        "ieai43wrv1 ss 09r0s 1b14oij70uu9 j2z9zu16bta cwk8p41jb y nmjxlws9",
                        "o6s 2ys3 kr9x6xqu5rpa5xjmmca19n khznppiqbam9wz bn4u6ole9wy7yxd869uyd79p7t11wepn3 uxh78kz7wbvm",
                        "h5 lkwji9axrh875rl",
                        "hbuyv6y6ma241d37tpb5btlxh542a0s2w9eq8riubmiv2s64vshhlbv0yv1ecdopaw7x5ymvyixp8ayakh6r10yi7t03",
                        "0vme9t s4xl44x  c oni rw8g1m g6oc2o3jdcm0i4u1y4x6 b i4gtk 7 glerao 3rty s5c54uddn2t95",
                        "0ek185xzbdlxbj8ci012nglwcbr19u7vvwfexec s0autjoj2ot99n2zt0g44y5y3hgz5",
                        "tf4rry2ru8wlf03vi8c1yi5vk8vaz19tqguukjey 6xs6as epal78hl4stg1t8634mc4v o7885"));
        Assert.assertEquals(records, store.search(key, query));
    }

    @Test
    @Theory
    @Ignore
    public void testSearchWithStopWordSubStringInQuery() {
        // TODO: Filed as CON-6 and CON-7. This should be fixed in an 0.2 update
        // release and 0.3
        add("string", Convert.javaToThrift("but foobar barfoo"), 1);
        Assert.assertTrue(store.search("string", "ut foobar barfoo")
                .contains(1));
    }

    @Test
    @Theory
    public void testSearchReproA(SearchType type) {
        String query = Variables.register("query", "tc e");
        String key = Variables.register("key", "2 6f0wzcw2ixa   dcf sa");
        Set<Long> records = setupSearchTest(
                key,
                query,
                type,
                Lists.newArrayList(6631928579149921621L),
                Lists.newArrayList("qi2sqa06xhn5quxbdasrtjsrzucbmo24fc4u78iv4rtc1ea7dnas74uxadvrf"));
        Assert.assertEquals(records, store.search(key, query));
    }

    @Test
    @Theory
    public void testSearchReproCON_2(SearchType type) {
        String query = Variables.register("query", "k i");
        String key = Variables.register("key", "oq99f7u7vizpob4o");
        Set<Long> records = setupSearchTest(
                key,
                query,
                type,
                Lists.newArrayList(1902752458578581194L, 1959599694661426747L,
                        -9154557670941699129L, -984115036014491508L,
                        5194905945498812204L, 5521526792899195281L,
                        4893428588236612746L, -6469751959947965328L,
                        -6053272917723840881L, -3780108145350335994L,
                        3428649801035140268L, 8581751009047755290L,
                        -8274546879060748693L, 4433133031784539226L,
                        2539259213139177697L),
                Lists.newArrayList(
                        "mbqg6 ls47i09bqt76lhtefjevbxt v9gc mqhily6agpweptt7dj jl a vbvqisuz4d xky18xs",
                        "9j6eue5m2 rn rovj4p4y bh kderm7xw40epfmbibtay6l2 x6 cw 833uhgkde43bwy8 b5u5rrlzdmqx",
                        "z22eb7likgl1xd1 bna93h2y  2nq  1kfejkmk iz22eb7likgl1xd1 bna93h2y  2nq  1kfejkm",
                        "n3a6dsk7gdhhp5re0piodkkvb085q7b7jj7bac0m27t6hhhajwyf",
                        "i4jfmy3nnfiupbnf04ecthucbj4pzisu4xpqy78k ii4jfmy3nnfiupbnf04ecthucbj4pzisu4xpqy78",
                        "b8yljef75 lvfwevcbb sg40mtoaovr2g8lgpgkcu88kprfdms7qncflm8wx0e9a9zt0zx8uvy4yf0mnqg",
                        "qfih uzg8 7 cy euxg 7sz8i8mj c40czvac6yk b worw65  3wkwhtc etulr1b9gsww puk iqfih uzg8 7 cy euxg 7sz8i8mj c40czvac6yk b worw65  3wkwhtc etulr1b9gsww pu",
                        "yte1xocdz agzid h3juda8fwpehyztqcc9ka2jb5",
                        "j1nl2lvd5ie",
                        "zqw e tfvd9y 4i7921apde59kfetaxcqcj89 s 1c5ncb t",
                        "simk a7 s7oh1 oz9wfrh7830q82hoorvfomcw8dzy9eaku cvu1pdknxwkcf1w9",
                        "eurti8wfy244clx15u",
                        "ig5 bq",
                        "y9rf7s 14y8o c8kraxfd714e9r9rqzq  ghoctaln2g 24dxirf ewwskvu5p7pn1h80s1nn fd88 z1c8k5dx7z0i5xhk iy9rf7s 14y8o c8kraxfd714e9r9rqzq  ghoctaln2g 24dxirf ewwskvu5p7pn1h80s1nn fd88 z1c8k5dx7z0i5xh",
                        "s93z3eggrxiuyb1enl59y  gwu7gn2cj 1luh j  pj"));
        Assert.assertEquals(records, store.search(key, query));
    }

    @Test
    public void testVerifyAfterAdd() {
        String key = TestData.getString();
        TObject value = TestData.getTObject();
        long record = TestData.getLong();
        add(key, value, record);
        Assert.assertTrue(store.verify(key, value, record));
    }

    @Test
    public void testVerifyAfterAddAndRemove() {
        String key = TestData.getString();
        TObject value = TestData.getTObject();
        long record = TestData.getLong();
        add(key, value, record);
        remove(key, value, record);
        Assert.assertFalse(store.verify(key, value, record));
    }

    @Test
    public void testVerifyAfterAddAndRemoveWithTime() {
        String key = TestData.getString();
        TObject value = TestData.getTObject();
        long record = TestData.getLong();
        add(key, value, record);
        long timestamp = Time.now();
        remove(key, value, record);
        Assert.assertTrue(store.verify(key, value, record, timestamp));
    }

    @Test
    public void testVerifyAfterAddWithTime() {
        String key = TestData.getString();
        TObject value = TestData.getTObject();
        long record = TestData.getLong();
        long timestamp = Time.now();
        add(key, value, record);
        Assert.assertFalse(store.verify(key, value, record, timestamp));
    }

    @Test
    public void testVerifyEmpty() {
        Assert.assertFalse(store.verify(TestData.getString(),
                TestData.getTObject(), TestData.getLong()));
    }

    /**
     * Add {@code key} as {@code value} to {@code record} in the {@code store}.
     * 
     * @param key
     * @param value
     * @param record
     */
    protected abstract void add(String key, TObject value, long record);

    /**
     * Cleanup the store and release and resources, etc.
     * 
     * @param store
     */
    protected abstract void cleanup(Store store);

    /**
     * Return a Store for testing.
     * 
     * @return the Store
     */
    protected abstract Store getStore();

    /**
     * Remove {@code key} as {@code value} from {@code record} in {@code store}.
     * 
     * @param key
     * @param value
     * @param record
     */
    protected abstract void remove(String key, TObject value, long record);

    /**
     * Add {@code key} as a value that satisfies {@code operator} relative to
     * {@code min}.
     * 
     * @param key
     * @param min
     * @param operator
     * @return the records added
     */
    private Set<Long> addRecords(String key, Number min, Operator operator) {
        Set<Long> records = getRecords();
        for (long record : records) {
            Number n = null;
            while (n == null
                    || (operator == Operator.GREATER_THAN && Numbers
                            .isLessThanOrEqualTo(n, min))
                    || (operator == Operator.GREATER_THAN_OR_EQUALS && Numbers
                            .isLessThan(n, min))
                    || (operator == Operator.LESS_THAN && Numbers
                            .isGreaterThanOrEqualTo(n, min))
                    || (operator == Operator.LESS_THAN_OR_EQUALS && Numbers
                            .isGreaterThan(n, min))
                    || (operator == Operator.NOT_EQUALS && Numbers.isEqualTo(n,
                            min))
                    || (operator == Operator.EQUALS && !Numbers.isEqualTo(n,
                            min))) {
                n = operator == Operator.EQUALS ? min : TestData.getNumber();
            }
            add(key, Convert.javaToThrift(n), record);
        }
        return records;
    }

    /**
     * Return a set of keys.
     * 
     * @return the keys.
     */
    private Set<String> getKeys() {
        Set<String> keys = Sets.newHashSet();
        for (int i = 0; i < TestData.getScaleCount(); i++) {
            String key = null;
            while (key == null || keys.contains(key)) {
                key = TestData.getString();
            }
            keys.add(key);
        }
        return keys;
    }

    /**
     * Return a set of primary keys.
     * 
     * @return the records
     */
    private Set<Long> getRecords() {
        Set<Long> records = Sets.newHashSet();
        for (int i = 0; i < TestData.getScaleCount(); i++) {
            Long record = null;
            while (record == null || records.contains(record)) {
                record = TestData.getLong();
            }
            records.add(record);
        }
        return records;
    }

    /**
     * Return a set of TObject values
     * 
     * @return the values
     */
    private Set<TObject> getValues() {
        Set<TObject> values = Sets.newHashSet();
        for (int i = 0; i < TestData.getScaleCount(); i++) {
            TObject value = null;
            while (value == null || values.contains(value)) {
                value = TestData.getTObject();
            }
            values.add(value);
        }
        return values;
    }

    /**
     * Remove {@code key} from a random sample of {@code records}.
     * 
     * @param key
     * @param records
     * @return the records that remain after the function
     */
    private Set<Long> removeRecords(String key, Set<Long> records) {
        Iterator<Long> it = records.iterator();
        while (it.hasNext()) {
            long record = it.next();
            if(TestData.getInt() % 3 == 0) {
                TObject value = store.fetch(key, record).iterator().next();
                it.remove();
                remove(key, value, record);
            }
        }
        return records;
    }

    /**
     * Setup a search test by adding some matches for {@code query} that
     * obey search {@code type} for {@code key} in some of the records from
     * {@code recordSource}.
     * 
     * @param key
     * @param query
     * @param type
     * @param recordSource
     * @param otherSource
     * @return the records where the query matches
     */
    private Set<Long> setupSearchTest(String key, String query,
            SearchType type, @Nullable Collection<Long> recordSource,
            @Nullable List<String> otherSource) {
        Preconditions.checkState(recordSource == null
                || (recordSource != null && otherSource != null && recordSource
                        .size() == otherSource.size()));
        Set<Long> records = Sets.newHashSet();
        recordSource = recordSource == null ? getRecords() : recordSource;
        if(!Strings.isNullOrEmpty(TStrings.stripStopWords(query))) {
            int i = 0;
            for (long record : recordSource) {
                if(otherSource != null) {
                    String other = otherSource.get(i);
                    boolean matches = TStrings.isInfixSearchMatch(
                            TStrings.stripStopWords(query),
                            TStrings.stripStopWords(other));
                    SearchTestItem sti = Variables.register("sti_" + record,
                            new SearchTestItem(key,
                                    Convert.javaToThrift(other), record, query,
                                    matches));
                    add(sti.key, sti.value, sti.record);
                    if(matches) {
                        records.add(sti.record);
                    }
                }
                else {
                    String other = null;
                    while (other == null
                            || other.equals(query)
                            || TStrings.isInfixSearchMatch(query, other)
                            || TStrings.isInfixSearchMatch(other, query)
                            || Strings.isNullOrEmpty(TStrings
                                    .stripStopWords(other))) {
                        other = TestData.getString();
                    }
                    boolean match = TestData.getInt() % 3 == 0;
                    if(match && type == SearchType.PREFIX) {
                        SearchTestItem sti = Variables.register(
                                "sti_" + record, new SearchTestItem(key,
                                        Convert.javaToThrift(query + other),
                                        record, query, true));
                        add(sti.key, sti.value, sti.record);
                        records.add(sti.record);
                    }
                    else if(match && type == SearchType.SUFFIX) {
                        SearchTestItem sti = Variables.register(
                                "sti_" + record, new SearchTestItem(key,
                                        Convert.javaToThrift(other + query),
                                        record, query, true));
                        add(sti.key, sti.value, sti.record);
                        records.add(sti.record);
                    }
                    else if(match && type == SearchType.INFIX) {
                        SearchTestItem sti = Variables.register(
                                "sti_" + record,
                                new SearchTestItem(key, Convert
                                        .javaToThrift(other + query + other),
                                        record, query, true));
                        add(sti.key, sti.value, sti.record);
                        records.add(sti.record);
                    }
                    else if(match && type == SearchType.FULL) {
                        SearchTestItem sti = Variables.register(
                                "sti_" + record, new SearchTestItem(key,
                                        Convert.javaToThrift(query), record,
                                        query, true));
                        add(sti.key, sti.value, sti.record);
                        records.add(sti.record);
                    }
                    else {
                        SearchTestItem sti = Variables.register(
                                "sti_" + record, new SearchTestItem(key,
                                        Convert.javaToThrift(other), record,
                                        query, false));
                        add(sti.key, sti.value, sti.record);
                    }
                }
                i++;
            }
            return records;
        }
        return records;

    }

    /**
     * Setup a search test by adding some random matches for {@code query} that
     * obey search {@code type} for {@code key} in a random set of records.
     * 
     * @param key
     * @param query
     * @param type
     * @return the records where the query matches
     */
    private Set<Long> setupSearchTest(String key, String query, SearchType type) {
        return setupSearchTest(key, query, type, null, null);
    }

    /**
     * An item that is used in a search test
     * 
     * @author jnelson
     */
    private class SearchTestItem {

        public final String query;
        public final String key;
        public final TObject value;
        public final long record;
        public final boolean match;

        /**
         * Construct a new instance
         * 
         * @param key
         * @param value
         * @param record
         * @param query
         * @param match
         */
        public SearchTestItem(String key, TObject value, long record,
                String query, boolean match) {
            this.key = key;
            this.value = value;
            this.record = record;
            this.query = query;
            this.match = match;
        }

        @Override
        public String toString() {
            return key + " AS " + value + " IN " + record
                    + (match ? " DOES" : " DOES NOT") + " MATCH " + query;
        }
    }

    /**
     * List of search types
     * 
     * @author jnelson
     */
    private enum SearchType {
        PREFIX, INFIX, SUFFIX, FULL
    }

}
