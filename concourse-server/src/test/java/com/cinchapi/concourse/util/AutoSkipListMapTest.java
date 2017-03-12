/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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
package com.cinchapi.concourse.util;

import java.util.Set;

import org.junit.Ignore;

import com.cinchapi.concourse.util.AutoMap;
import com.google.common.base.Function;

/**
 * 
 * 
 * @author Jeff Nelson
 */
@Ignore("not in use")
public class AutoSkipListMapTest extends AutoMapTest{

    @Override
    protected AutoMap<String, Set<String>> getInstance(
            Function<String, Set<String>> loader,
            Function<Set<String>, Boolean> cleaner) {
        return AutoMap.newAutoSkipListMap(loader, cleaner);
    }

}
