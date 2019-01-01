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
package com.cinchapi.concourse.server.io.process;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import spark.utils.IOUtils;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.common.base.Platform;
import com.cinchapi.common.process.Processes;
import com.cinchapi.common.process.Processes.ProcessResult;
import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.cinchapi.concourse.util.Commands;
import com.cinchapi.concourse.util.TestData;

/**
 * Unit tests for {@link JavaApp}.
 * 
 * @author Jeff Nelson
 */
public class JavaAppTest extends ConcourseBaseTest {

    /**
     * The expected out for the {@link #GOOD_SOURCE}.
     */
    private static final String EXPECTED_OUTPUT = TestData.getString();

    /**
     * Source code that should compile and run correctly.
     */
    private static final String GOOD_SOURCE = ""
        // @formatter:off
        + "public class GoodSource {"
        + "  public static void main(String... args) {"
        + "    System.out.println(\"" + EXPECTED_OUTPUT + "\");"
        + "  }" 
        + "}";
        // @formatter:on

    /**
     * Source code that does not compile.
     */
    private static final String BAD_SOURCE = ""
        // @formatter:off
        + "public class GoodSource {"
        + "  public static void main(String... args) {"
        + "    Systems.out.println(\"" + EXPECTED_OUTPUT + "\");"
        + "  }" 
        + "}";
        // @formatter:on

    @Test
    public void testGoodSource() throws InterruptedException, IOException {
        JavaApp app = new JavaApp(GOOD_SOURCE);
        app.run();
        app.waitFor();
        Assert.assertEquals(0, app.exitValue());
        Assert.assertEquals(EXPECTED_OUTPUT,
                IOUtils.toString(app.getInputStream()).trim());
    }

    @Test(expected = Exception.class)
    public void testBadSource() {
        JavaApp app = new JavaApp(BAD_SOURCE);
        try {
            app.compile();
        }
        catch (Exception e) {
            System.out.println("[INFO] You can ignore the error message above");
            throw e;
        }
    }

    @Test
    @Ignore
    public void testPrematureShutdownHandler() {
        // See if 'jps' is available
        try {
            Commands.jps();
        }
        catch (Exception e) {
            Assert.assertTrue(true);
        }
        int interval = JavaApp.PREMATURE_SHUTDOWN_CHECK_INTERVAL_IN_MILLIS;
        try {
            JavaApp.PREMATURE_SHUTDOWN_CHECK_INTERVAL_IN_MILLIS = 100;
            final AtomicBoolean passed = new AtomicBoolean(false);
            final AtomicBoolean ranHook = new AtomicBoolean(false);
            JavaApp app = new JavaApp("public class GoodSource {"
                    + "public static void main(String... args){"
                    + "while(true){continue;}" + "}" + "}");
            app.onPrematureShutdown(new PrematureShutdownHandler() {

                @Override
                public void run(InputStream out, InputStream err) {
                    passed.set(true);
                    ranHook.set(true);

                }

            });
            app.run();
            String procs = Commands.jps();
            String parts[] = procs.split(System.lineSeparator());
            String pid = "";
            while (pid.isEmpty()) {
                for (String part : parts) {
                    if(part.contains("GoodSource")) {
                        pid = part.split("GoodSource")[0].trim();
                        break;
                    }
                }
            }
            Commands.run("kill -9 " + pid);
            long start = System.currentTimeMillis();
            while (!ranHook.get()) { // wait for the hook to run
                if(System.currentTimeMillis()
                        - start < JavaApp.PREMATURE_SHUTDOWN_CHECK_INTERVAL_IN_MILLIS
                                + 100) {
                    continue;
                }
                else {
                    break;
                }
            }
            Assert.assertTrue(passed.get());
        }
        finally {
            JavaApp.PREMATURE_SHUTDOWN_CHECK_INTERVAL_IN_MILLIS = interval;
        }

    }

    @Test
    @Ignore
    public void testPid() {
        if(Platform.isLinux() || Platform.isMacOsX() || Platform.isSolaris()) {
            String id = UUID.randomUUID().toString();
            JavaApp app = new JavaApp(System.getProperty("java.class.path"),
                    GOOD_SOURCE,
                    "-Dcom.cinchapi.concourse.server.io.process.pid=" + id);
            app.run();
            ProcessBuilder pb = Processes
                    .getBuilderWithPipeSupport("ps aux | grep " + id
                            + " | grep -v \"grep\" | awk '{print $2}' ");
            try {
                Process process = pb.start();
                ProcessResult result = Processes
                        .waitForSuccessfulCompletion(process);
                long expected = Long.parseLong(result.out().get(0));
                Assert.assertEquals(expected, app.pid());
            }
            catch (IOException e) {
                throw CheckedExceptions.throwAsRuntimeException(e);
            }
        }
        else {
            Assert.assertTrue(true);
        }

    }

}
