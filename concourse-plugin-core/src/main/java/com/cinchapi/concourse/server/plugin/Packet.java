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
package com.cinchapi.concourse.server.plugin;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import com.cinchapi.concourse.annotate.PackagePrivate;
import com.cinchapi.concourse.thrift.TObject;
import com.google.common.collect.Lists;

/**
 * TODO add some docs
 * 
 * @author Jeff Nelson
 */
public class Packet implements Serializable {

    private static final long serialVersionUID = 9214118090555607982L;

    private final List<Data> data;

    public Packet() {
        this.data = Lists.newArrayList();
    }

    public List<Data> getData() {
        return Collections.unmodifiableList(data);
    }

    class Data {
        private final ModType type;
        private final String key;
        private final TObject value;
        private final long record;
        private final long timestamp;

        public Data(ModType type, String key, TObject value, long record,
                long timestamp) {
            this.type = type;
            this.key = key;
            this.value = value;
            this.record = record;
            this.timestamp = timestamp;
        }
    }

    protected enum ModType {
        ADD, REMOVE
    }

}
