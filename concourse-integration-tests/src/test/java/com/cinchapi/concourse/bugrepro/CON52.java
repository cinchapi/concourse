/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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
package com.cinchapi.concourse.bugrepro;

import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.util.StandardActions;

/**
 * Repro of <a href="https://cinchapi.atlassian.net/browse/CON-52">CON-52</a>
 * where search can temporarily return inconsistent results when
 * data is being transported from the buffer to the database.
 * 
 * 
 * @author Jeff Nelson
 */
public class CON52 extends ConcourseIntegrationTest {

    @Test
    public void test() {
        StandardActions.import1027YoutubeLinks(client);
        for (int i = 0; i < 20; i++) {
            int size = client.search("youtube_embed_link", "youtube").size();
            Assert.assertEquals(size, 1027);
            StandardActions.wait(5, TimeUnit.MILLISECONDS); // slight delay to
                                                            // give data time to
                                                            // transport
        }

    }

}
