/*
 * Copyright (c) 2013-2015 Cinchapi, Inc.
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
package org.cinchapi.concourse.util;

/**
 * 
 * 
 * @author Jeff Nelson
 */
public final class Arrays {

    private Arrays() {/* noop */}

    /**
     * Render an array as a string that looks like it would if the array were
     * actually an {@link ArrayList}.
     * 
     * @param array
     * @return the string representation
     */
    public static <T> String toString(T[] array) {
        StringBuilder builder = new StringBuilder();
        builder.append("[");
        for (T item : array) {
            if(item != null) {
                builder.append(item).append(",");
            }
        }
        builder.deleteCharAt(builder.length() - 1);
        builder.append("]");
        return builder.toString();
    }

}
