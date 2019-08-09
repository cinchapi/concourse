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
package com.cinchapi.concourse.exporter;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Assert.*;
import org.junit.Test;

import com.cinchapi.concourse.cli.presentation.IO;

public final class ExportCliTest {
    private static final class TestIO implements IO {
        final List<String> responses;
        int i = -1;

        TestIO(List<String> responses) {
            this.responses = responses;
        }

        @Override
        public String readLine(String output, Character mask) {
            i++;
            try {
                return responses.get(i);
            }
            catch (ArrayIndexOutOfBoundsException e) {
                i = 0;
                return responses.get(i);
            }
        }

        @Override
        public void setExpandEvents(boolean expand) {}
    }

    @Test
    public void testHelp() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        System.setOut(new PrintStream(out));
        String result = "";
        try {
            new ExportCli(new String[] { "--help" },
                    new TestIO(Arrays.asList("admin", "admin")));
        }
        catch (Exception e) {
            result = out.toString();
        }

        Assert.assertTrue(result.startsWith(
                "Usage: export-cli [options] additional program arguments..."));

    }

    @Test
    public void test() {

    }
}
