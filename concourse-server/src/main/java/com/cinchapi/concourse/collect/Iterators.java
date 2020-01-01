/*
 * Copyright (c) 2013-2020 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse.collect;

import java.io.IOException;
import java.util.Iterator;

import com.cinchapi.common.base.CheckedExceptions;

/**
 * Utility class for {@link Iterator}s.
 *
 * @author Jeff Nelson
 */
public final class Iterators {

    /**
     * Close the {@code it}erator if it can be {@link CloseableIterator closed}.
     * 
     * @param it
     */
    public static <T> void close(Iterator<T> it) {
        if(it instanceof CloseableIterator) {
            try {
                ((CloseableIterator<T>) it).close();
            }
            catch (IOException e) {
                throw CheckedExceptions.wrapAsRuntimeException(e);
            }
        }
    }

    private Iterators() {/* no-init */}

}
