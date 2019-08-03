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
package com.cinchapi.concourse.util;

import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.google.common.collect.Sets;

/**
 * Unit tests for the {@link Transformers} class.
 * 
 * @author chandresh.pancholi
 */
public class TransformersTest extends ConcourseBaseTest {

    @Test
    public void testLazyTransformSet() {
        int count = Random.getScaleCount();
        Set<String> original = Sets.newHashSet();
        for (int i = 0; i < count; ++i) {
            original.add(Random.getString());
        }

        Assert.assertEquals(
                Transformers.transformSet(original, StringUtils::reverse),
                Transformers.transformSetLazily(original,
                        StringUtils::reverse));
    }

}
