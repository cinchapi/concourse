/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013 Jeff Nelson, Cinchapi Software Collective
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.cinchapi.concourse.server.storage;

import java.util.Iterator;
import java.util.Set;

import org.cinchapi.concourse.server.GlobalState;
import org.cinchapi.concourse.server.model.Position;
import org.cinchapi.concourse.server.model.PrimaryKey;
import org.cinchapi.concourse.server.model.Text;
import org.cinchapi.concourse.server.model.Value;
import org.cinchapi.concourse.testing.Variables;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.Convert;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

/**
 * 
 * 
 * @author jnelson
 */
public class SearchBlockTest extends BlockTest<Text, Text, Position> {

	@Override
	@Test(expected = IllegalStateException.class)
	public void testCannotInsertInImmutableBlock() {
		Text locator = Variables.register("locator", getLocator());
		Value value = Variables.register("value", getStringValue());
		PrimaryKey record = Variables.register("record", getRecord());
		((SearchBlock) block).insert(locator, value, record, Time.now(),
				Action.ADD);
		if(block.size() <= 0) {
			value = Variables.register("value", getStringValue());
			((SearchBlock) block).insert(locator, value, record, Time.now(),
					Action.ADD);
		}
		block.sync();
		((SearchBlock) block).insert(locator, value, record, Time.now(),
				Action.ADD);
	}

	private Value getStringValue() {
		return Value.wrap(Convert.javaToThrift(TestData.getString()));
	}

	private PrimaryKey getRecord() {
		return TestData.getPrimaryKey();
	}

	@Override
	@Test
	public void testMightContainLocatorKeyValue() {
		Value value = null;
		Text term = null;
		int position = 0;
		while (term == null) {
			value = Value.wrap(Convert.javaToThrift(TestData.getString()));
			position = 0;
			for (String string : value.getObject().toString().split(" ")) {
				string = string.trim();
				if(!GlobalState.STOPWORDS.contains(string)) {
					term = Text.wrap(string);
					break;
				}
				else {
					position++;
					continue;
				}
			}
		}
		doTestMightContainLocatorKeyValue(getLocator(), value, term,
				getRecord(), position);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testMightContainLocatorKeyValueReproCON_1() {
		doTestMightContainLocatorKeyValue(
				Text.wrap("eqcicldw12dsowa7it4vi0pnqgewxci4c3ihyzf"),
				Value.wrap(Convert
						.javaToThrift("w jvnwa8xofm6asavrgpyxpk1mbgah7slcaookolqo fpa3g5 5csjly")),
				Text.wrap("w"), PrimaryKey.wrap(52259011321627880L), 0);
	}

	@Test
	public void testReproCON_4() {
		// TODO file this in jira
		Text key = Variables.register("key", Text.wrap("strings"));
		PrimaryKey record = Variables.register("record", getRecord());
		Value value = Variables
				.register(
						"value",
						Value.wrap(Convert
								.javaToThrift("aaihwopxetdxrumqlbjwgdsjgs tan rczlfjhyhlwhsr aqzpmquui mmmynpklmctgnonaaafagpjgv augolkz")));
		((SearchBlock) block)
				.insert(key, value, record, Time.now(), Action.ADD);
		Text term = Variables.register("term", Text.wrap("aa"));
		Variables.register("blockDump", block.dump());
		SearchRecord searchRecord = Record.createSearchRecordPartial(key, term);
		((SearchBlock) block).seek(key, term, searchRecord);
		Assert.assertTrue(searchRecord.search(term).contains(record));
	}

	@Test
	public void testDoesNotAddDuplicates() {
		// LINE 20:
		// imwhrxxhtysldepivwwpbererstvplxnoknicpboajbdoayadaceldzbeasolxrnxcizcjjvymugsqyotcefeoohggsxaapnc
		// LINE 29:
		// rsikwrnyuvpxwufblpqxyhsmbphrepiickfmzivktvxoxfjrmwbmtbtkvczyptcgkpogdlnydqaatbsrhkyjrgjuyixyhtdngowj
//		Text key = Text.wrap("strings");
//		long i = 1;
//		Iterator<String> it = TestData.getWordsDotTxt().iterator();
//		while (i <= 29 && it.hasNext()) {
//			PrimaryKey record = PrimaryKey.wrap(i);
//			Value value = Value.wrap(Convert.javaToThrift(it.next()));
//			((SearchBlock) block).insert(key, value, record, Time.now(),
//					Action.ADD);
//			i++;
//		}
//		String[] lines = block.dump().split("\n");
//		Set<String> set = Sets.newHashSet();
//		for (String line : lines) {
//			set.add(line);
//		}
//		Assert.assertEquals(lines.length, set.size());
	}

	/**
	 * The implementation of {@link #testMightContainLocatorKeyValue()}.
	 * 
	 * @param locator
	 * @param value
	 * @param term
	 * @param record
	 * @param position
	 */
	private void doTestMightContainLocatorKeyValue(Text locator, Value value,
			Text term, PrimaryKey record, int position) {
		Preconditions.checkArgument(!GlobalState.STOPWORDS.contains(term
				.toString()));
		Variables.register("locator", locator);
		Variables.register("value", value);
		Variables.register("term", term);
		Variables.register("record", record);
		Assert.assertFalse(block.mightContain(locator, term,
				Position.wrap(record, position)));
		((SearchBlock) block).insert(locator, value, record, Time.now(),
				Action.ADD);
		Assert.assertTrue(block.mightContain(locator, term,
				Position.wrap(record, position)));
	}

	@Override
	protected Text getLocator() {
		return TestData.getText();
	}

	@Override
	protected Text getKey() {
		return TestData.getText();
	}

	@Override
	protected Position getValue() {
		return TestData.getPosition();
	}

	@Override
	protected SearchBlock getMutableBlock(String directory) {
		return Block.createSearchBlock(Long.toString(Time.now()), directory);
	}

}
