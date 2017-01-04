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
package com.cinchapi.concourse;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.util.Strings;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.gson.JsonObject;

/**
 * Unit tests for the {@code insert()} API methods
 * 
 * @author Jeff Nelson
 */
public class InsertTest extends ConcourseIntegrationTest {

    @Test
    public void testInsertInteger() {
        JsonObject object = new JsonObject();
        String key = "foo";
        int value = 1;
        object.addProperty(key, value);
        String json = object.toString();
        long record = client.insert(json).iterator().next();
        Assert.assertEquals(value, (int) client.get(key, record));
    }

    @Test
    public void testInsertTagStripsBackticks() { // CON-157
        JsonObject object = new JsonObject();
        String key = "__section__";
        String value = "com.cinchapi.concourse.oop.Person";
        object.addProperty(key, "`" + value + "`");
        String json = object.toString();
        long record = client.insert(json).iterator().next();
        Assert.assertEquals(value, client.get(key, record));
    }

    @Test
    public void testInsertBoolean() {
        JsonObject object = new JsonObject();
        String key = "foo";
        boolean value = false;
        object.addProperty(key, value);
        String json = object.toString();
        long record = client.insert(json).iterator().next();
        Assert.assertEquals(value, client.get(key, record));
    }

    @Test
    public void testInsertBooleanAsTag() {
        JsonObject object = new JsonObject();
        String key = "foo";
        boolean value = false;
        object.addProperty(key, "`" + value + "`");
        String json = object.toString();
        long record = client.insert(json).iterator().next();
        Assert.assertEquals(Boolean.toString(value), client.get(key, record));
    }

    @Test
    public void testInsertLink() {
        JsonObject object = new JsonObject();
        object.addProperty("spouse", Link.to(1));
        String json = object.toString();
        client.insert(json, 2);
        Assert.assertTrue(client.find("spouse", Operator.LINKS_TO, 1).contains(
                2L));
    }

    @Test
    public void testInsertResolvableLink() {
        client.set("name", "Jeff", 1);
        JsonObject object = new JsonObject();
        object.addProperty("name", "Ashleah");
        object.addProperty("spouse", Link.toWhere("name = Jeff"));
        String json = object.toString();
        client.insert(json, 2);
        Assert.assertTrue(client.find("spouse", Operator.LINKS_TO, 1).contains(
                2L));
    }

    @Test
    public void testInsertResolvableLinkIntoNewRecord() {
        client.set("name", "Jeff", 1);
        JsonObject object = new JsonObject();
        object.addProperty("name", "Ashleah");
        object.addProperty("spouse", Link.toWhere("name = Jeff"));
        String json = object.toString();
        long record = client.insert(json).iterator().next();
        Assert.assertTrue(client.find("spouse", Operator.LINKS_TO, 1).contains(
                record));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testInsertResolvableLinkWithLocalTargets() {
        Multimap<String, Object> a = HashMultimap.create();
        a.put("foo", 20);
        Multimap<String, Object> b = HashMultimap.create();
        b.put("bar", Link.toWhere("foo < 50"));
        b.put("_id", 1);
        client.insert(Lists.newArrayList(a, b));
        long record = Iterables.getOnlyElement(client.find("foo = 20"));
        Assert.assertEquals(Sets.newHashSet(Iterables.getOnlyElement(client
                .find("_id = 1"))), client.find(Strings.format("bar lnks2 {}",
                record)));
    }

    @Test
    public void testInsertMultimap() {
        Multimap<String, Object> map = HashMultimap.create();
        map.put("name", "Jeff Nelson");
        map.put("company", "Cinchapi");
        map.put("title", "CEO");
        map.put("direct_reports", Link.to(1));
        map.put("direct_reports", Link.to(2));
        long record = client.insert(map);
        Assert.assertEquals("Jeff Nelson", client.get("name", record));
        Assert.assertEquals("CEO", client.get("title", record));
        Assert.assertEquals("Cinchapi", client.get("company", record));
        Assert.assertEquals(Sets.newLinkedHashSet(Lists.newArrayList(
                Link.to(2), Link.to(1))), client.select("direct_reports",
                record));
    }

    @Test
    public void testInsertMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("name", "Jeff Nelson");
        map.put("company", "Cinchapi");
        map.put("title", "CEO");
        map.put("direct_reports", Lists.newArrayList(Link.to(1), Link.to(2)));
        long record = client.insert(map);
        Assert.assertEquals("Jeff Nelson", client.get("name", record));
        Assert.assertEquals("CEO", client.get("title", record));
        Assert.assertEquals("Cinchapi", client.get("company", record));
        Assert.assertEquals(Sets.newLinkedHashSet(Lists.newArrayList(
                Link.to(2), Link.to(1))), client.select("direct_reports",
                record));
    }

    @Test
    public void testInsertMultimaps() {
        Multimap<String, Object> a = HashMultimap.create();
        Multimap<String, Object> b = HashMultimap.create();
        Multimap<String, Object> c = HashMultimap.create();
        a.put("foo", "bar");
        a.put("foo", "baz");
        a.put("bar", 1);
        a.put("baz", true);
        a.put("baz", Link.to(50));
        b.put("name", "Jeff Nelson");
        b.put("company", "Cinchapi");
        b.put("title", "CEO");
        b.put("direct_reports", Link.to(2));
        b.put("direct_reports", Link.to(1));
        c.put("pi", (double) (22 / 7));
        List<Multimap<String, Object>> list = Lists.newArrayList();
        list.add(a);
        list.add(b);
        list.add(c);
        Set<Long> records = client.insert(list);
        long record1 = Iterables.get(records, 0);
        long record2 = Iterables.get(records, 1);
        long record3 = Iterables.get(records, 2);
        Assert.assertEquals("baz", client.get("foo", record1));
        Assert.assertEquals("Cinchapi", client.get("company", record2));
        Assert.assertEquals(Link.to(2), client.get("direct_reports", record2));
        Assert.assertEquals(22 / 7, (double) client.get("pi", record3), 0);
    }

    @Test
    public void testInsertMultimapIntoRecords() {
        Multimap<String, Object> map = HashMultimap.create();
        map.put("name", "Jeff Nelson");
        map.put("company", "Cinchapi");
        map.put("title", "CEO");
        map.put("direct_reports", Link.to(1));
        map.put("direct_reports", Link.to(2));
        long record1 = TestData.getLong();
        long record2 = TestData.getLong();
        client.insert(map, Lists.newArrayList(record1, record2));
        Assert.assertEquals(client.select(record1), client.select(record2));
    }

    @Test
    public void testInsertMapIntoRecords() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("name", "Jeff Nelson");
        map.put("company", "Cinchapi");
        map.put("title", "CEO");
        map.put("direct_reports", Sets.newHashSet(Link.to(1), Link.to(2)));
        long record1 = TestData.getLong();
        long record2 = TestData.getLong();
        client.insert(map, Lists.newArrayList(record1, record2));
        Assert.assertEquals(client.select(record1), client.select(record2));
    }

    @Test
    public void testInsertMultimapIntoRecord() {
        Multimap<String, Object> map = HashMultimap.create();
        map.put("name", "Jeff Nelson");
        map.put("company", "Cinchapi");
        map.put("title", "CEO");
        map.put("direct_reports", Link.to(1));
        map.put("direct_reports", Link.to(2));
        long record = TestData.getLong();
        Assert.assertTrue(client.insert(map, record));
        Assert.assertEquals("Jeff Nelson", client.get("name", record));
        Assert.assertEquals("CEO", client.get("title", record));
        Assert.assertEquals("Cinchapi", client.get("company", record));
        Assert.assertEquals(Sets.newLinkedHashSet(Lists.newArrayList(
                Link.to(2), Link.to(1))), client.select("direct_reports",
                record));
    }

    @Test
    public void testInsertMapIntoRecord() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("name", "Jeff Nelson");
        map.put("company", "Cinchapi");
        map.put("title", "CEO");
        map.put("direct_reports", Lists.newArrayList(Link.to(1), Link.to(2)));
        long record = TestData.getLong();
        Assert.assertTrue(client.insert(map, record));
        Assert.assertEquals("Jeff Nelson", client.get("name", record));
        Assert.assertEquals("CEO", client.get("title", record));
        Assert.assertEquals("Cinchapi", client.get("company", record));
        Assert.assertEquals(Sets.newLinkedHashSet(Lists.newArrayList(
                Link.to(2), Link.to(1))), client.select("direct_reports",
                record));
    }

    @Test
    public void testInsertFailsIfSomeDataAlreadyExists() {
        Multimap<String, Object> map = HashMultimap.create();
        map.put("name", "Jeff Nelson");
        map.put("company", "Cinchapi");
        map.put("title", "CEO");
        map.put("direct_reports", Link.to(1));
        map.put("direct_reports", Link.to(2));
        long record = TestData.getLong();
        client.add("name", "Jeff Nelson", record);
        Assert.assertFalse(client.insert(map, record));
    }

    @Test(expected = Exception.class)
    // @Test(expected = InvalidArgumentException.class) //TODO CON-460
    public void testInsertJsonArrayReproA() {
        String json = "[{\"id\":34,\"handle\":\".tp-caption.medium_bg_orange\",\"settings\":\"{\\\"hover\\\":\\\"false\\\"}\",\"hover\":\"\",\"params\":'{\"color\":\"rgb(255, 255, 255)\",\"font-size\":\"20px\",\"line-height\":\"20px\",\"font-weight\":\"800\",\"font-family\":\"\\\"Open Sans\\\"\",\"text-decoration\":\"none\",\"padding\":\"10px\",\"background-color\":\"rgb(243, 156, 18)\",\"border-width\":\"0px\",\"border-color\":\"rgb(255, 214, 88)\",\"border-style\":\"none\"}',\"__table\":\"wp_revslider_css\"}]";
        Set<Long> records = client.insert(json);
        Assert.assertFalse(records.isEmpty());
    }

}
