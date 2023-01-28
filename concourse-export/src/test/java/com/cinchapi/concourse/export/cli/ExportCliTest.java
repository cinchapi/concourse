/*
 * Copyright (c) 2013-2023 Cinchapi Inc.
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
package com.cinchapi.concourse.export.cli;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.base.AnyObjects;
import com.cinchapi.common.base.Array;
import com.cinchapi.common.base.ArrayBuilder;
import com.cinchapi.common.base.QuoteAwareStringSplitter;
import com.cinchapi.common.base.SplitOption;
import com.cinchapi.concourse.importer.CsvImporter;
import com.cinchapi.concourse.importer.Importer;
import com.cinchapi.concourse.lang.sort.Sort;
import com.cinchapi.concourse.test.ClientServerTest;
import com.cinchapi.concourse.util.FileOps;
import com.cinchapi.concourse.util.Resources;
import com.google.common.collect.ImmutableSet;

/**
 * Unit tests for {@link ExportCli}.
 *
 * @author Jeff Nelson
 */
public class ExportCliTest extends ClientServerTest {

    /**
     * File where data is exported during a test
     */
    String output;

    @Override
    public void beforeEachTest() {
        String file = Resources.getAbsolutePath("/college.csv");
        Importer importer = new CsvImporter(client);
        importer.importFile(file);
        output = FileOps.tempFile();
    }

    /**
     * Transform the {@code args} into a complete set arguments to use in a CLI
     * test.
     * 
     * @param args
     * @return the CLI args
     */
    private String[] generateCliArgs(String... args) {
        args = AnyObjects
                .split(args, v -> new QuoteAwareStringSplitter(v, ' ',
                        SplitOption.TRIM_WHITESPACE, SplitOption.DROP_QUOTES))
                .toArray(Array.containing());
        ArrayBuilder<String> ab = ArrayBuilder.builder();
        ab.add("--username");
        ab.add("admin");
        ab.add("--password");
        ab.add("admin");
        ab.add("--port");
        ab.add(String.valueOf(server.getClientPort()));
        ab.add("--file");
        ab.add(output);
        ab.add(args);
        return ab.build();
    }

    @Test
    public void testParseRecordsArgs() {
        String[] args = generateCliArgs(
                "-r 1 2 3 -r 4,5, 6,7 -r 8 -r 9 10 -r 11");
        ExportCli cli = new ExportCli(args);
        Assert.assertEquals(
                ImmutableSet.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L),
                cli.records);
    }

    @Test
    public void testExportOrder() {
        String[] args = generateCliArgs(
                "--select undergraduate_population --order undergraduate_population");
        ExportCli cli = new ExportCli(args);
        cli.exec();
        Collection<Object> expected = client.get("undergraduate_population",
                client.inventory(), Sort.by("undergraduate_population"))
                .values();
        List<Integer> actual = $output().stream().skip(1)
                .map(line -> line.split("\\,")[0]).map(Integer::parseInt)
                .collect(Collectors.toList());
        Iterator<Object> eit = expected.iterator();
        Iterator<Integer> ait = actual.iterator();
        while (eit.hasNext()) {
            Assert.assertEquals(eit.next(), ait.next());
        }
    }

    @Test
    public void testExportSize() {
        String[] args = generateCliArgs(
                "--select undergraduate_population --size 10");
        ExportCli cli = new ExportCli(args);
        cli.exec();
        Assert.assertEquals(11, $output().size()); // size includes the header
    }

    @Test
    public void testExportWhere() {
        String[] args = generateCliArgs(
                "--where \"percent_undergrad_asian > 50\"");
        ExportCli cli = new ExportCli(args);
        cli.exec();
        Map<Long, Object> data = client.get("ipeds_id",
                "percent_undergrad_asian > 50");
        String output = FileOps.read(this.output);
        data.forEach((record, ipedsId) -> {
            Assert.assertTrue(output.contains(ipedsId.toString()));
        });
    }

    /**
     * Return the exported output.
     * 
     * @return the exported output
     */
    private List<String> $output() {
        return FileOps.readLines(output);
    }

    @Override
    protected String getServerVersion() {
        return ClientServerTest.LATEST_SNAPSHOT_VERSION;
    }

}
