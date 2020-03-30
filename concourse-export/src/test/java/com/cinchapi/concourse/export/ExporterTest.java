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
package com.cinchapi.concourse.export;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Unit tests for {@link Printer}.
 *
 * @author jeff
 */
public class ExporterTest {

    @Test
    public void outputTest() {
        List<Map<String, Object>> stuff = ImmutableList.of(
                ImmutableMap.of("a", "1", "b", 2),
                ImmutableMap.of("b", 2, "c", 3, "d", 4),
                ImmutableMap.of("a", ImmutableList.of(1, 2, 3)));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream stream = new PrintStream(out);
        Exporters.csv(stream).export(stuff);
        StringBuilder expected = new StringBuilder();
        expected.append("a,b,c,d").append(System.lineSeparator())
                .append("1,2,null,null").append(System.lineSeparator())
                .append("null,2,3,4").append(System.lineSeparator())
                .append("\"1,2,3\",null,null,null");
        Assert.assertEquals(expected.toString(), out.toString());
    }
}