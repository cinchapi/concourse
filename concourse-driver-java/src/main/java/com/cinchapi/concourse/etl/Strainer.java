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
package com.cinchapi.concourse.etl;

import java.util.Map;
import java.util.function.BiConsumer;

import com.cinchapi.common.collect.Sequences;

/**
 * A {@link Strainer} is used to process a generic data set (e.g. a {@link Map}
 * from {@link String} to {@link Object}) using an {@link BiConsumer action}.
 * <p>
 * {@link Strainer Strainers} work in accordance with Concourse's data model,
 * which is a non-nestable, implicit multimap. Therefore, a {@link Strainer}
 * flattens top level {@link Sequences#isSequence(Object) sequence} values and
 * process their items individually.
 * </p>
 *
 * @author Jeff Nelson
 */
public class Strainer {

    /**
     * The action to perform on each key/value pair.
     */
    private final BiConsumer<String, Object> action;

    /**
     * Construct a new instance.
     * 
     * @param action
     */
    public Strainer(BiConsumer<String, Object> action) {
        this.action = action;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Strainer) {
            return action.equals(((Strainer) obj).action);
        }
        else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return action.hashCode();
    }

    /**
     * Process each key/value pair in {@code data}.
     * <p>
     * If a value is a {@link Sequences#isSequence(Object) sequence}, it is
     * "flattened" and the assessment is applied to each of its members. This is
     * done because Concourse's data model is an non-nestable implicit multimap.
     * </p>
     * 
     * @param key
     * @param value
     */
    public final void process(Map<String, Object> data) {
        data.forEach((key, value) -> process(key, value));
    }

    /**
     * Process the key/value pair.
     * <p>
     * If a value is a {@link Sequences#isSequence(Object) sequence}, it is
     * "flattened" and the assessment is applied to each of its members. This is
     * done because Concourse's data model is an non-nestable implicit multimap.
     * </p>
     * 
     * @param key
     * @param value
     */
    public final void process(String key, Object value) {
        if(Sequences.isSequence(value)) {
            Sequences.forEach(value, item -> action.accept(key, item));
        }
        else {
            action.accept(key, value);
        }
    }
}