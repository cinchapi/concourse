/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2015 Jeff Nelson, Cinchapi Software Collective
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
package org.cinchapi.concourse;

/**
 * The base class for all exceptions that happen during (staged) operations in
 * a transaction.
 * <p>
 * All operations that occur within a transaction should be wrapped in a
 * try-catch block so that transaction exceptions can be caught and the
 * transaction can be properly aborted.
 * 
 * <pre>
 * try {
 *     concourse.stage();
 *     concourse.get(&quot;foo&quot;, 1);
 *     concourse.add(&quot;foo&quot;, &quot;bar&quot;, 1);
 *     concourse.commit();
 * }
 * catch (TransactionException e) {
 *     concourse.abort();
 * }
 * </pre>
 * 
 * </p>
 * <p>
 * <em>Please note that this and all descendant exceptions are unchecked for
 * backwards compatibility, but they may be changed to be checked in a future
 * API breaking release.</em>
 * </p>
 * 
 * @author jnelson
 */
// TODO change to extend Exception instead of RuntimeException
public class TransactionException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public TransactionException() {
        super("Another client has made changes to data used "
                + "within the current transaction, so it cannot "
                + "continue. Please abort the transaction and try again.");
    }

}
