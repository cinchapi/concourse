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
package com.cinchapi.concourse.server.storage.temp;

import com.cinchapi.concourse.annotate.PackagePrivate;

/**
 * An unchecked exception that is thrown when an attempt is made to insert a
 * {@link Write} into a {@link Limbo} that does not have enough
 * capacity.
 * 
 * @author Jeff Nelson
 */
@PackagePrivate
class CapacityException extends RuntimeException {

    /**
     * A cached instance of the exception that can be used to avoid the overhead
     * that is traditionally associated with creating new exceptions. This is
     * okay to do since this exception is used as a state signaler and does not
     * really imply any sort of error case.
     */
    static final CapacityException INSTANCE = new CapacityException();

    private static final long serialVersionUID = 1L;

}
