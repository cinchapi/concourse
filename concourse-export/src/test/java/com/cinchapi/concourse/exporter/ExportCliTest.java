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
import java.util.List;
import java.util.function.Supplier;

import org.junit.Assert;
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

    private PrintStream console = System.out;

    private Supplier<String> getOutput() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        System.setOut(new PrintStream(out));
        return out::toString;
    }

    @Test
    public void testHelp() {
        Supplier<String> output = getOutput();
        String result = "";

        try {
            new ExportCli(new String[] { "--help" });
        }
        catch (Exception e) {
            result = output.get();
        }

        console.println(result);

        Assert.assertTrue(result.startsWith(
                "Usage: export-cli2 [options] additional program arguments..."));
    }

    @Test
    public void test() {
        Supplier<String> output = getOutput();
        new ExportCli(new String[] {
                "-u", "admin",
                "--password", "admin",
                "--records", "1", "2", "3",
                "-ccl", "testin"
        }).doTask();

        Assert.assertTrue(output.get().contains("1"));
    }
}
