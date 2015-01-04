package org.cinchapi.concourse;

import org.cinchapi.concourse.thrift.Operator;
import org.junit.Assert;
import org.junit.Test;

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
        client.add("cmd", "cd /home/applications/eclipse/", 1);
        client.add("cmd", "cd /home/desktop", 2);
        client.add("cmd", "vim /var/www/html/index.html", 3);
        client.add("cmd", "cat /home/notes.txt", 4);
        client.add("cmd", "find / -name home", 5);
        client.add("cmd", "ls -la /HOME/", 6);
        client.add("cmd", "mkdir home_directory", 7);
        client.add("cmd", "cp /var/www/home123 /opt/home", 8);
        Assert.assertEquals(client.find("cmd", Operator.LINKS_TO, "/home/"),
                client.find("cmd", Operator.EQUALS, "/home/"));
    }
}
