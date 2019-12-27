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
package com.cinchapi.concourse;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.test.ConcourseIntegrationTest;

/**
 * Unit test for API method to undo changes of data in a record.
 */
public class UndoTest extends ConcourseIntegrationTest {

    @Test
    public void testUndo() {
        client.set("name", "Nick", 1);
        client.set("name", "Jeff", 1);
        Assert.assertEquals("Record 1 `name` does not equal Jeff.", "Jeff", client.get("name", 1));
        client.undo(1, "name", 1);
        Assert.assertEquals("Record 1 `name` does not equal Nick after invocation of undo(changes, key, record).", "Nick", client.get("name", 1));
    }

}
