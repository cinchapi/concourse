/*
 * Copyright (c) 2013-2023 Cinchapi Inc.
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
package com.cinchapi.common.util;

import java.io.Closeable;
import java.io.IOException;

/**
 * A utility class to handle objects that may implement the {@link Closeable}
 * interface.
 *
 * @author Jeff Nelson
 */
public class PossibleCloseables {

    /**
     * If {@code object} is a {@link Closeable}, {@link Closeable#close() close}
     * it without propagating any exceptions.
     * 
     * @param object
     */
    public static void tryCloseQuietly(Object object) {
        if(object instanceof Closeable) {
            try {
                ((Closeable) object).close();
            }
            catch (IOException e) {/* swallow */}
        }
    }

}