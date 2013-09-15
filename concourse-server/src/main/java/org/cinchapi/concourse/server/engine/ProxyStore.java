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

import org.cinchapi.common.annotate.DoNotInvoke;
import org.cinchapi.common.annotate.PackagePrivate;
import org.cinchapi.concourse.thrift.TObject;

/**
 * A {@link Store} that is a write proxy for a {@link PermanentStore}. This
 * service has the ability to directly process writes and eventually transport
 * them to a {@link PermanentStore}.
 * 
 * @author jnelson
 */
@PackagePrivate
interface ProxyStore extends WritableStore {

	/**
	 * Insert a write without performing validity checks in situations where the
	 * normal checks are not appropriate and the caller has other means to
	 * ensure the validity of the revision (i.e. a {@link BufferedStore} that
	 * must resolve reads from two sources). This method should
	 * only be invoked from authorized callers.
	 * 
	 * @param key
	 * @param value
	 * @param record
	 * @return {@code true}
	 */
	@DoNotInvoke
	public boolean addUnsafe(String key, TObject value, long record);

	/**
	 * Insert a write without performing validity checks in situations where the
	 * normal checks are not appropriate and the caller has other means to
	 * ensure the validity of the revision (i.e. a {@link BufferedStore} that
	 * must resolve reads from two sources). This method should
	 * only be invoked from authorized callers.
	 * 
	 * @param key
	 * @param value
	 * @param record
	 * @return {@code true}
	 */
	@DoNotInvoke
	public boolean removeUnsafe(String key, TObject value, long record);

	/**
	 * Transfer the content to {@code destination}.
	 * 
	 * @param destination
	 */
	public void transport(PermanentStore destination);
}
