/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
 * @author Jeff Nelson
 */
public class TransactionException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public TransactionException() {
        super("Another client has made changes to data used "
                + "within the current transaction, so it cannot "
                + "continue. Please abort the transaction and try again.");
    }

}
