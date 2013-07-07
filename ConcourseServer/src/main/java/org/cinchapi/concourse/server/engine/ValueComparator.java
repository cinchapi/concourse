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

import java.util.Comparator;

import org.cinchapi.common.annotate.PackagePrivate;
import org.cinchapi.common.tools.Numbers;
import org.cinchapi.concourse.Convert;

/**
 * A {@link Comparator} that sorts {@link Value} objects logically using weak
 * typing.
 * 
 * @see {@link Value#compareToLogically(Value)}
 * @author jnelson
 */
@PackagePrivate
class ValueComparator implements Comparator<Value> {

	@Override
	public int compare(Value o1, Value o2) {
		Object q1 = Convert.thriftToJava(o1.getQuantity());
		Object q2 = Convert.thriftToJava(o2.getQuantity());
		if(q1 instanceof Number && q2 instanceof Number) {
			return Numbers.compare((Number) q1, (Number) q2);
		}
		else {
			return q1.toString().compareTo(q2.toString());
		}
	}

}
