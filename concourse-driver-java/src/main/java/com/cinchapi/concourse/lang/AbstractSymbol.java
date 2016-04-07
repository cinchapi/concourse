/*
 * Copyright (c) 2013-2016 Cinchapi Inc.
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
package com.cinchapi.concourse.lang;

/**
 * An abstract implementation of the {@link Symbol} interface that provides
 * {@link #hashCode()} and {@link #equals(Object)}.
 * 
 * @author Jeff Nelson
 */
class AbstractSymbol implements Symbol {

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if(obj.getClass() == getClass()) {
            return toString().equals(obj.toString());
        }
        else {
            return false;
        }
    }

}
