/*
 * Copyright (c) 2013-2016 Cinchapi Inc.
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
package com.cinchapi.concourse.plugin.data;

import io.atomix.catalyst.buffer.Buffer;

import java.util.Map;
import java.util.Set;

import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.Transformers;
import com.google.common.collect.Maps;

/**
 * A {@link ResultDataset} that wraps a {@link TObjectDataset} and lazily
 * transforms values.
 * 
 * @author Jeff Nelson
 */
public class ObjectResultDataset extends ResultDataset<Object> {

    /**
     * The internal dataset that contains the data.
     */
    private Dataset<Long, String, TObject> data;

    /**
     * Construct a new instance.
     * 
     * @param data
     */
    public ObjectResultDataset(Dataset<Long, String, TObject> data) {
        this.data = data;
    }

    @Override
    public Set<Object> get(Long entity, String attribute) {
        return Transformers.transformSetLazily(data.get(entity, attribute), (
                item) -> Convert.javaToThrift(item));
    }

    @Override
    public Map<String, Set<Object>> get(Object entity) {
        Map<String, Set<Object>> result = Maps.newLinkedHashMap();
        data.get(entity).forEach(
                (key, value) -> {
                    result.put(key, Transformers.transformSetLazily(value, (
                            item) -> Convert.javaToThrift(item)));
                });
        return result;
    }

    @Override
    protected Object deserializeValue(Buffer buffer) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void serializeValue(Object value, Buffer buffer) {
        throw new UnsupportedOperationException();
    }

}
