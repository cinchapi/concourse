package com.cinchapi.concourse.exporter;

import com.cinchapi.concourse.cli.presentation.IO;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;

public final class ExportCliTest {
    private static final class TestIO implements IO {
        final List<String> responses;
        int i = -1;

        TestIO(List<String> responses) { this.responses = responses; }

        @Override
        public String readLine(String output, Character mask) {
            i++;
            try {
                return responses.get(i);
            } catch (ArrayIndexOutOfBoundsException e) {
                i = 0;
                return responses.get(i);
            }
        }

        @Override
        public void setExpandEvents(boolean expand) { }
    }

    @Test
    public void testHelp() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        System.setOut(new PrintStream(out));
        String result = "";
        try {
            new ExportCli(new String[] { "--help" },
                    new TestIO(Arrays.asList("admin", "admin")));
        } catch (Exception e) {
            result = out.toString();
        }

        Assert.assertTrue(result.startsWith(
                "Usage: export-cli [options] additional program arguments..."));



    }
    @Test
    public void test() {

    }
}
