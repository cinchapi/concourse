package org.cinchapi.concourse;

import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.util.TestData;
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
		String key = TestData.getString();
		int record = TestData.getScaleCount();
		client.add(key, 1, record);
		Assert.assertEquals(client.find(key, Operator.LIKE, record),
				client.find(key, Operator.REGEX, record));
	}

	@Test
	public void testNotLikeAlias() {
		String key = TestData.getString();
		int record = TestData.getScaleCount();
		client.add(key, 1, record);
		Assert.assertEquals(client.find(key, Operator.NOT_LIKE, record),
				client.find(key, Operator.NOT_REGEX, record));
	}

	@Test
	public void testLinkToAlias() {
		String key = TestData.getString();
		int record = TestData.getScaleCount();
		client.add(key, 1, record);
		Assert.assertEquals(client.find(key, Operator.LINKS_TO, record),
				client.find(key, Operator.EQUALS, record));
	}
}
