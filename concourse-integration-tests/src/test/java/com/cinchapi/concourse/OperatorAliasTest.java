/*
 * Licensed to Cinchapi Inc, under one or more contributor license 
 * agreements. See the NOTICE file distributed with this work for additional 
 * information regarding copyright ownership. Cinchapi Inc. licenses this 
 * file to you under the Apache License, Version 2.0 (the "License"); you may 
 * not use this file except in compliance with the License. You may obtain a 
 * copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

import com.cinchapi.concourse.Link;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.thrift.Operator;

/**
 * Unit tests the following alias operators: LIKE, NOT_LIKE and LINK_TO.
 * 
 * @author remie
 *
 */
public class OperatorAliasTest extends ConcourseIntegrationTest {

    @Test
    public void testLikeAlias() {
        client.add("cmd", "cd /home/applications/eclipse/", 1);
        client.add("cmd", "cd /home/desktop", 2);
        client.add("cmd", "vim /var/www/html/index.html", 3);
        client.add("cmd", "cat /home/notes.txt", 4);
        client.add("cmd", "find / -name home", 5);
        client.add("cmd", "ls -la /HOME/", 6);
        client.add("cmd", "mkdir home_directory", 7);
        client.add("cmd", "cp /var/www/home123 /opt/home", 8);
        Assert.assertEquals(client.find("cmd", Operator.LIKE, "%/home/%"),
                client.find("cmd", Operator.REGEX, "%/home/%"));
    }

    @Test
    public void testNotLikeAlias() {
        client.add("cmd", "cd /home/applications/eclipse/", 1);
        client.add("cmd", "cd /home/desktop", 2);
        client.add("cmd", "vim /var/www/html/index.html", 3);
        client.add("cmd", "cat /home/notes.txt", 4);
        client.add("cmd", "find / -name home", 5);
        client.add("cmd", "ls -la /HOME/", 6);
        client.add("cmd", "mkdir home_directory", 7);
        client.add("cmd", "cp /var/www/home123 /opt/home", 8);
        Assert.assertEquals(client.find("cmd", Operator.NOT_LIKE, "%/home/%"),
                client.find("cmd", Operator.NOT_REGEX, "%/home/%"));
    }

    @Test
    public void testLinkToAlias() {
        client.link("foo", 1, 2);
        client.link("foo", 3, 2);
        client.link("foo", 4, 2);
        client.link("foo", 5, 2);
        Assert.assertEquals(client.find("cmd", Operator.LINKS_TO, 2),
                client.find("cmd", Operator.EQUALS, Link.to(2)));
    }
}
