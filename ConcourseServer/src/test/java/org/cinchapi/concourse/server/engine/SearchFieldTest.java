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
package org.cinchapi.concourse.server.engine;

import org.cinchapi.common.io.Byteable;
import org.cinchapi.common.util.Tests;

/**
 * 
 * 
 * @author jnelson
 */
public class SearchFieldTest extends FieldTest<Text, Position> {

	@SuppressWarnings("unchecked")
	@Override
	protected Field<Text, Position> copy(Byteable object) {
		Field<Text, Position> field = (Field<Text, Position>) object;
		return new SearchField(field); // authorized
	}

	@Override
	protected Field<Text, Position> newInstance(Text key) {
		return new SearchField(key); // authorized
	}

	@Override
	protected Text getKey() {
		return Text.fromString(Tests.getString());
	}

	@Override
	protected Position getForStorageValue() {
		return Position.fromPrimaryKeyAndMarker(
				PrimaryKey.forStorage(Tests.getLong()),
				Math.abs(Tests.getInt()));
	}

	@Override
	protected Position getForStorageValueCopy(Position value) {
		return Position.fromPrimaryKeyAndMarker(
				PrimaryKey.forStorage(value.getPrimaryKey().longValue()),
				value.getPosition());
	}

	@Override
	protected Position getNotForStorageValue() {
		return Position.fromPrimaryKeyAndMarker(
				PrimaryKey.notForStorage(Tests.getLong()),
				Math.abs(Tests.getInt()));
	}

}
