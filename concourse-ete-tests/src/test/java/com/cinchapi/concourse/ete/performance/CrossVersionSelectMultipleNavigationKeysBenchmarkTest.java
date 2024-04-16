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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.cinchapi.common.profile.Benchmark;
import com.google.common.collect.ImmutableList;

/**
 * Unit tests for selecting multiple navigation keys
 * 
 * @author Jeff Nelson
 */
public class CrossVersionSelectMultipleNavigationKeysBenchmarkTest
        extends CrossVersionBenchmarkTest {

    List<Long> records = new ArrayList<>();
    List<String> keys;
    {
        long record = 1;
        while (records.size() < 5000) {
            records.add(record);
            record += 7;
        }
        keys = ImmutableList.of("job.title", "job.company.name", "student.name",
                "student.email", "job.company.website", "association",
                "foo.bar", "student.association", "job.company.$id$",
                "student.name.$id$", "job.company");
    }

    @Override
    protected void beforeEachBenchmarkRuns() {
        for (long record : records) {
            long offer = record + 1;
            long student = record + 2;
            long job = record + 3;
            long company = record + 4;
            long company2 = record + 7;
            long association = record + 5;
            long details = record + 6;
            client.add("name", "Association", association);
            client.add("name", "Company", company);
            client.add("website", "Website", company);
            client.add("name", "Company2", company2);
            client.add("website", "Website", company2);
            client.add("name", "Student", student);
            client.add("name", "Good Student", student);
            client.add("email", "Email", student);
            client.add("title", "Title", job);
            client.link("company", company, job);
            client.link("company", company2, job);
            client.link("association", association, offer);
            client.link("job", job, offer);
            client.link("student", student, offer);
            client.link("details", details, offer);
        }
    }

    @Test
    public void testSelectNavigationKeys() {
        Benchmark benchmark = new Benchmark(TimeUnit.MILLISECONDS) {

            @Override
            public void action() {
                client.select(keys, records);
            }

        };
        long elapsed = benchmark.run();
        record("select", elapsed);
    }

}
