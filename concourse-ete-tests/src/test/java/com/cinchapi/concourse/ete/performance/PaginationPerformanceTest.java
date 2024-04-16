/*
 * Copyright (c) 2013-2024 Cinchapi Inc.
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
package com.cinchapi.concourse.ete.performance;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.base.Resources;
import com.cinchapi.common.profile.Benchmark;
import com.cinchapi.concourse.importer.CsvImporter;
import com.cinchapi.concourse.importer.Importer;
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.test.ClientServerTest;

/**
 * Unit tests to validate the performance of pagination
 *
 * @author Jeff Nelson
 */
public class PaginationPerformanceTest extends ClientServerTest {

    @Override
    protected String getServerVersion() {
        return ClientServerTest.LATEST_SNAPSHOT_VERSION;
    }

    @Test
    public void testPaginationDoesNotLoadEntireResultSet() {
        Importer importer = new CsvImporter(client);
        Set<Long> records = importer
                .importFile(Resources.get("/generated.csv").getFile());
        server.stop();
        server.start();
        client = server.connect();
        Benchmark all = new Benchmark(TimeUnit.MILLISECONDS) {

            @Override
            public void action() {
                client.select(records);
            }

        };
        long allTime = all.run();
        server.stop();
        server.start();
        client = server.connect();
        Benchmark paginated = new Benchmark(TimeUnit.MILLISECONDS) {

            @Override
            public void action() {
                client.select(records, Page.sized(100).go(90));
            }

        };
        long paginatedTime = paginated.run();
        System.out.println(allTime);
        System.out.println(paginatedTime);
        Assert.assertTrue(paginatedTime < allTime);
    }

}
