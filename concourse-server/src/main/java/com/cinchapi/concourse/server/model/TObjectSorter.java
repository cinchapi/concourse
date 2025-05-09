/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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
package com.cinchapi.concourse.server.model;

import java.util.Comparator;

import com.cinchapi.concourse.thrift.TObject;

/**
 * A {@link Comparator} that is used to sort TObjects using weak typing.
 * 
 * @author Jeff Nelson
 */
public enum TObjectSorter implements Comparator<TObject> {
    INSTANCE;

    @Override
    public int compare(TObject v1, TObject v2) {
        // This logic features a bit of misdirection, but it's intentional. Yes,
        // the Value.Sorter ultimately uses the TObject comparator, but, in
        // addition to storing a reference to the wrapped TObject, Value's cache
        // themselves on the TObject to ensure instances are deduplicated across
        // the JVM. So, converting TObject's to Values here allows us to take
        // advantage of that while maintaining one logical codepath for
        // TObject/Value equality, which should always behave the same.
        return Value.Sorter.INSTANCE.compare(Value.wrap(v1), Value.wrap(v2));
    }
}
