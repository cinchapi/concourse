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
import org.cinchapi.concourse.util.Convert;

/**
 * Unit tests for {@link SecondaryField}.
 * 
 * @author jnelson
 */
public class SecondaryFieldTest extends FieldTest<Value, PrimaryKey>{

	@Override
	protected Field<Value, PrimaryKey> copy(Byteable object) {
		return new SecondaryField((SecondaryField)object); //authorized
	}

	@Override
	protected Field<Value, PrimaryKey> newInstance(Value key) {
		return new SecondaryField(key); //authorized
	}

	@Override
	protected Value getKey() {
		return Value.forStorage(Convert.javaToThrift(Tests.getObject()));
	}

	@Override
	protected PrimaryKey getForStorageValue() {
		return PrimaryKey.forStorage(Tests.getLong());
	}

	@Override
	protected PrimaryKey getForStorageValueCopy(PrimaryKey value) {
		return PrimaryKey.forStorage(value.longValue());
	}

	@Override
	protected PrimaryKey getNotForStorageValue() {
		return PrimaryKey.notForStorage(Tests.getLong());
	}

}
