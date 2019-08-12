/*
 * Copyright (c) 2013-2019 Cinchapi Inc.
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
package com.cinchapi.concourse.exporter.helpers;

import java.util.Map;

public final class Tuple<T, Q> implements Map.Entry<T, Q> {
    final T _1;
    final Q _2;

    public Tuple(T t, Q q) {
        _1 = t;
        _2 = q;
    }

    @Override
    public T getKey() {
        return _1;
    }

    @Override
    public Q getValue() {
        return _2;
    }

    @Override
    public Q setValue(Q value) {
        throw new RuntimeException("Tuples are immutable.");
    }
}
