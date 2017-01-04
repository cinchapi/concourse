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
package com.cinchapi.concourse.server.storage;

/**
 * An unchecked exception that is thrown when an attempt is made to operate on a
 * closed {@link Transaction}. This class exists to differentiate failures in a
 * transaction from those in an atomic operation because it is possible that the
 * caller may want to auto retry a failed atomic operation whereas a failed
 * transaction cannot be auto retried.
 * 
 * @author Jeff Nelson
 */
public class TransactionStateException extends AtomicStateException {

    private static final long serialVersionUID = 1L;

}
