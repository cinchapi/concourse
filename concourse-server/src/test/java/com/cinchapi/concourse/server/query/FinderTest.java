/*
 * Copyright (c) 2013-2018 Cinchapi Inc.
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
package com.cinchapi.concourse.server.query;

import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.ccl.Parser;
import com.cinchapi.ccl.syntax.AbstractSyntaxTree;
import com.cinchapi.concourse.server.storage.temp.Queue;
import com.cinchapi.concourse.server.storage.temp.Write;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.Parsers;
import com.google.common.collect.Sets;

/**
 * Unit tests for {@link Finder}.
 * 
 * @author Jeff Nelson
 */
public class FinderTest {

    @Test
    public void testBasicExpression() {
        Queue store = new Queue(16);
        store.insert(Write.add("name", Convert.javaToThrift("jeff"), 1));
        store.insert(Write.add("company", Convert.javaToThrift("Cinchapi"), 1));
        store.insert(Write.add("age", Convert.javaToThrift(100), 1));
        store.insert(Write.add("name", Convert.javaToThrift("jeff"), 2));
        store.insert(Write.add("company", Convert.javaToThrift("Blavity"), 2));
        store.insert(Write.add("age", Convert.javaToThrift(100), 2));
        store.insert(Write.add("name", Convert.javaToThrift("ashleah"), 3));
        store.insert(
                Write.add("company", Convert.javaToThrift("ARMN Inc."), 3));
        store.insert(Write.add("age", Convert.javaToThrift(50), 3));
        String ccl = "name = jeff";
        Parser parser = Parsers.create(ccl);
        AbstractSyntaxTree ast = parser.parse();
        Finder visitor = Finder.instance();
        Set<Long> result = ast.accept(visitor, store);
        Assert.assertEquals(Sets.newHashSet(1L, 2L), result);
    }

    @Test
    public void testAndConjunction() {
        Queue store = new Queue(16);
        store.insert(Write.add("name", Convert.javaToThrift("jeff"), 1));
        store.insert(Write.add("company", Convert.javaToThrift("Cinchapi"), 1));
        store.insert(Write.add("age", Convert.javaToThrift(100), 1));
        store.insert(Write.add("name", Convert.javaToThrift("jeff"), 2));
        store.insert(Write.add("company", Convert.javaToThrift("Blavity"), 2));
        store.insert(Write.add("age", Convert.javaToThrift(100), 2));
        store.insert(Write.add("name", Convert.javaToThrift("ashleah"), 3));
        store.insert(
                Write.add("company", Convert.javaToThrift("ARMN Inc."), 3));
        store.insert(Write.add("age", Convert.javaToThrift(50), 3));
        String ccl = "name = jeff and company = Cinchapi";
        Parser parser = Parsers.create(ccl);
        AbstractSyntaxTree ast = parser.parse();
        Finder visitor = Finder.instance();
        Set<Long> result = ast.accept(visitor, store);
        Assert.assertEquals(Sets.newHashSet(1L), result);
    }

    @Test
    public void testOrConjunction() {
        Queue store = new Queue(16);
        store.insert(Write.add("name", Convert.javaToThrift("jeff"), 1));
        store.insert(Write.add("company", Convert.javaToThrift("Cinchapi"), 1));
        store.insert(Write.add("age", Convert.javaToThrift(100), 1));
        store.insert(Write.add("name", Convert.javaToThrift("jeff"), 2));
        store.insert(Write.add("company", Convert.javaToThrift("Blavity"), 2));
        store.insert(Write.add("age", Convert.javaToThrift(100), 2));
        store.insert(Write.add("name", Convert.javaToThrift("ashleah"), 3));
        store.insert(
                Write.add("company", Convert.javaToThrift("ARMN Inc."), 3));
        store.insert(Write.add("age", Convert.javaToThrift(50), 3));
        String ccl = "name = jeff or age < 75";
        Parser parser = Parsers.create(ccl);
        AbstractSyntaxTree ast = parser.parse();
        Finder visitor = Finder.instance();
        Set<Long> result = ast.accept(visitor, store);
        Assert.assertEquals(Sets.newHashSet(1L, 2L, 3L), result);
    }

    @Test
    public void testAndConjunctionShortCircuit() {
        Queue store = new Queue(16);
        store.insert(Write.add("name", Convert.javaToThrift("jeff"), 1));
        store.insert(Write.add("company", Convert.javaToThrift("Cinchapi"), 1));
        store.insert(Write.add("age", Convert.javaToThrift(100), 1));
        store.insert(Write.add("name", Convert.javaToThrift("jeff"), 2));
        store.insert(Write.add("company", Convert.javaToThrift("Blavity"), 2));
        store.insert(Write.add("age", Convert.javaToThrift(100), 2));
        store.insert(Write.add("name", Convert.javaToThrift("ashleah"), 3));
        store.insert(
                Write.add("company", Convert.javaToThrift("ARMN Inc."), 3));
        store.insert(Write.add("age", Convert.javaToThrift(50), 3));
        String ccl = "(name = jeff or company = Cinchapi) and age = 70";
        Parser parser = Parsers.create(ccl);
        AbstractSyntaxTree ast = parser.parse();
        Finder visitor = Finder.instance();
        Set<Long> result = ast.accept(visitor, store);
        Assert.assertEquals(Sets.newHashSet(), result);
    }

}
