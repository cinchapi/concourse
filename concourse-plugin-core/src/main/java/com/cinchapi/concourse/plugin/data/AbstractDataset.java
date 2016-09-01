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

import java.util.Map;
import java.util.Set;

public abstract class AbstractDataset<V> extends Dataset<Long, String, V> {

    private static final long serialVersionUID = -1612162452019105991L;

    @Override
    protected Map<V, Set<Long>> createInvertedMultimap() {
        return TrackingLinkedHashMultimap.create();
    }

}
