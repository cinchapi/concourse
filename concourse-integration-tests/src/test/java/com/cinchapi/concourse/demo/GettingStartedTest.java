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
package com.cinchapi.concourse.demo;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.time.Time;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * A test that follows the Concourse Getting Started guide at
 * https://cinchapi.atlassian.net/wiki/display/CON/Getting+Started.
 * 
 * @author Jeff Nelson
 */
public class GettingStartedTest extends ConcourseIntegrationTest {

    @Test
    public void testGettingStarted() {
        // Add
        Assert.assertTrue(client.add("name", "John Doe", 1));
        Assert.assertTrue(client.add("name", "Johnny Doe", 1));
        Assert.assertTrue(client.add("name", "Jonathan Doe", 1));
        Assert.assertTrue(client.add("name", "J. Doe", 1));

        Assert.assertTrue(client.add("age", 30, 1));
        Assert.assertTrue(client.add("age", 30.5F, 1));
        Assert.assertTrue(client.add("age", "30", 1));
        Assert.assertTrue(client.add("age", true, 1));

        // Remove
        Assert.assertTrue(client.remove("age", true, 1));

        // Fetch
        Assert.assertEquals(Sets.newHashSet("John Doe", "Johnny Doe",
                "Jonathan Doe", "J. Doe"), client.select("name", 1));

        // Get
        Assert.assertEquals("J. Doe", client.get("name", 1));

        // Set
        for (int i = 0; i < 5; i++) {
            client.add("baz", i, 1);
        }
        Assert.assertEquals(Sets.newHashSet(0, 1, 2, 3, 4),
                client.select("baz", 1));
        client.set("baz", 6, 1);
        Assert.assertEquals(Sets.newHashSet(6), client.select("baz", 1));

        // Describe
        Assert.assertEquals(Sets.newHashSet("name", "age", "baz"),
                client.describe(1));

        // Verify
        Assert.assertTrue(client.verify("age", 30, 1));

        // Find
        for (int i = 0; i <= 1000; i++) {
            client.add("count", i, i);
        }
        Set<Long> set = client.find("count", Operator.BETWEEN, 100, 300);
        for (long i = 100; i < 300; i++) {
            Assert.assertTrue(set.contains(i));
        }

        // Audit
        Iterator<String> it = client.audit(1).values().iterator();
        Assert.assertTrue(it.next().startsWith(
                "ADD name AS John Doe (STRING) IN 1"));
        Assert.assertTrue(it.next().startsWith(
                "ADD name AS Johnny Doe (STRING) IN 1"));
        Assert.assertTrue(it.next().startsWith(
                "ADD name AS Jonathan Doe (STRING) IN 1"));
        Assert.assertTrue(it.next().startsWith(
                "ADD name AS J. Doe (STRING) IN 1"));
        Assert.assertTrue(it.next().startsWith("ADD age AS 30 (INTEGER) IN 1"));
        Assert.assertTrue(it.next().startsWith("ADD age AS 30.5 (FLOAT) IN 1"));
        Assert.assertTrue(it.next().startsWith("ADD age AS 30 (STRING) IN 1"));
        Assert.assertTrue(it.next()
                .startsWith("ADD age AS true (BOOLEAN) IN 1"));
        Assert.assertTrue(it.next().startsWith(
                "REMOVE age AS true (BOOLEAN) IN 1"));
        Assert.assertTrue(it.next().startsWith("ADD baz AS 0 (INTEGER) IN 1"));
        Assert.assertTrue(it.next().startsWith("ADD baz AS 1 (INTEGER) IN 1"));
        Assert.assertTrue(it.next().startsWith("ADD baz AS 2 (INTEGER) IN 1"));
        Assert.assertTrue(it.next().startsWith("ADD baz AS 3 (INTEGER) IN 1"));
        Assert.assertTrue(it.next().startsWith("ADD baz AS 4 (INTEGER) IN 1"));
        Assert.assertTrue(it.next()
                .startsWith("REMOVE baz AS 0 (INTEGER) IN 1"));
        Assert.assertTrue(it.next()
                .startsWith("REMOVE baz AS 1 (INTEGER) IN 1"));
        Assert.assertTrue(it.next()
                .startsWith("REMOVE baz AS 2 (INTEGER) IN 1"));
        Assert.assertTrue(it.next()
                .startsWith("REMOVE baz AS 3 (INTEGER) IN 1"));
        Assert.assertTrue(it.next()
                .startsWith("REMOVE baz AS 4 (INTEGER) IN 1"));
        Assert.assertTrue(it.next().startsWith("ADD baz AS 6 (INTEGER) IN 1"));
        Assert.assertTrue(it.next().startsWith("ADD count AS 1 (INTEGER) IN 1"));

        List<String> audit = Lists.newArrayList(client.audit(1).values());
        it = client.audit("age", 1).values().iterator();
        Assert.assertEquals(it.next(), audit.get(4));
        Assert.assertEquals(it.next(), audit.get(5));
        Assert.assertEquals(it.next(), audit.get(6));
        Assert.assertEquals(it.next(), audit.get(7));
        Assert.assertEquals(it.next(), audit.get(8));

        // Revert
        Iterator<Timestamp> it2 = client.audit(1).keySet().iterator();
        for (int i = 0; i < 11; i++) {
            it2.next();
        };
        Timestamp t1 = it2.next();

        client.revert("baz", 1, t1);
        Assert.assertEquals(Sets.newHashSet(0, 1, 2), client.select("baz", 1));

        // Historical Describe
        Assert.assertTrue(client.describe(1,
                Timestamp.fromMicros(Time.now() - 86400000000L)).isEmpty());

        it2 = client.audit(1).keySet().iterator();
        for (int i = 0; i < 8; i++) {
            it2.next();
        };
        t1 = it2.next();
        Assert.assertEquals(Sets.newHashSet("name", "age"),
                client.describe(1, t1));

        // Historical Fetch
        it2 = client.audit(1).keySet().iterator();
        for (int i = 0; i < 1; i++) {
            it2.next();
        };
        t1 = it2.next();
        Assert.assertEquals(Sets.newHashSet("John Doe", "Johnny Doe"),
                client.select("name", 1, t1));

        // Historical Find
        t1 = client.audit(50).keySet().iterator().next();
        Assert.assertTrue(client.find("count", Operator.GREATER_THAN, 50, t1)
                .isEmpty());

        t1 = client.audit(500).keySet().iterator().next();
        set = client.find("count", Operator.GREATER_THAN, 50, t1);
        for (long i = 51; i <= 500; i++) {
            Assert.assertTrue(set.contains(i));
        }

        // Historical Verify
        it2 = client.audit("age", 1).keySet().iterator();
        for (int i = 0; i < 3; i++) {
            it2.next();
        };
        t1 = it2.next();
        Assert.assertTrue(client.verify("age", true, 1, t1));

        // Search
        String[] strings = { "The Cat in the Hat", "Green Eggs and Ham",
                "Horton Hears a Who", "The Cat in the Hat Comes Back",
                "Scrambled Eggs Super" };

        Set<Long> expected = Sets.newHashSet();
        for (int i = 0; i < 1000; i++) {
            String value = strings[i % strings.length];
            client.add("title", value, i);
            if(value.contains("eggs")) {
                expected.add((long) i);
            }
        }
        Set<Long> actual = client.search("title", "eggs");
        for (Long record : expected) {
            Assert.assertTrue(actual.contains(record));
        }

    }
}
