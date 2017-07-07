# Copyright (c) 2013-2017 Cinchapi Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# This file contains a collection of Concourse Server exceptions that are
# gracefully throwable by the Thrift API. Unlike other parts of the Thrift API,
# these structs are considered to be part of the end-driver API and will be used
# directly by users.
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# To generate java source code run:
# thrift -out concourse-driver-java/src/main/java -gen java interface/exceptions.thrift
namespace java com.cinchapi.concourse.thrift

# To generate python source code run:
# thrift -out concourse-driver-python -gen py interface/exceptions.thrift
namespace py concourse.thriftapi.exceptions

# To generate PHP source code run:
# thrift -out concourse-driver-php/src -gen php interface/exceptions.thrift
namespace php concourse.thrift.exceptions

# To generate Ruby source code run:
# thrift -out concourse-driver-ruby/lib/ -gen rb:namespaced interface/exceptions.thrift
namespace rb concourse

/**
 * Signals that an attempt to conditionally add or insert data based on a
 * condition that should be unique, cannot happen because the condition is not
 * unique.
 */
exception DuplicateEntryException {
    1: string message
}

/**
 * Signals that an invalid argument was submitted.
 */
exception InvalidArgumentException {
    1: string message
}

/**
 * Signals that an unexpected or invalid token was reached while parsing.
 */
exception ParseException {
    1: string message
}

/**
 * Signals that a security violation has occurred and the currently running
 * session must end immediately.
 */
exception SecurityException {
    1: string message
}

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
 */
exception TransactionException {}

/**
 * Thrown when a managed operation fails.
 */
exception ManagementException {
    1: string message
}
