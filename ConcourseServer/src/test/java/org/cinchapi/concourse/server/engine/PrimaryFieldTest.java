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
import org.cinchapi.common.util.Random;
import org.cinchapi.concourse.util.Convert;

/**
 * Unit tests for {@link PrimaryField}.
 * 
 * @author jnelson
 */
public class PrimaryFieldTest extends FieldTest<Text, Value> {

	@Override
	protected Field<Text, Value> copy(Byteable object) {
		return new PrimaryField((PrimaryField) object);
	}

	@Override
	protected Field<Text, Value> newInstance(Text key) {
		return new PrimaryField(key); // authorized;
	}

	@Override
	protected Text getKey() {
		return Text.fromString(Random.getString());
	}

	@Override
	protected Value getForStorageValue() {
		return Value.forStorage(Convert.javaToThrift(Random.getObject()));
	}

	@Override
	protected Value getForStorageValueCopy(Value value) {
		return Value.forStorage(value.getQuantity());
	}

	@Override
	protected Value getNotForStorageValue() {
		return Value.notForStorage(Convert.javaToThrift(Random.getObject()));
	}

}
