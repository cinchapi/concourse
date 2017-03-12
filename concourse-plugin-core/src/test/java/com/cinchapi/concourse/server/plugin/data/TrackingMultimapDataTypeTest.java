/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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
package com.cinchapi.concourse.server.plugin.data;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Link;
import com.cinchapi.concourse.server.plugin.data.TrackingMultimap.DataType;
import com.cinchapi.concourse.util.Random;

/**
 * Tests to determine whether the
 * {@link TrackingMultimap#percentKeyDataType(DataType)} method works as
 * intended. Also tests for the
 * {@link TrackingMultimap#containsDataType(DataType)}
 * method, which is related.
 *
 */
public class TrackingMultimapDataTypeTest
        extends TrackingMultimapBaseTest<Object, Object> {

    @Test
    public void testBoolean() {
        double percentBools = calculateManualPercentByClass(Boolean.class);
        Assert.assertEquals(percentBools,
                map.percentKeyDataType(DataType.BOOLEAN), 0);
    }

    @Test
    public void testString() {
        double percentStrings = calculateManualPercentByClass(String.class);
        Assert.assertEquals(percentStrings,
                map.percentKeyDataType(DataType.STRING), 0);
    }

    @Test
    public void testNumber() {
        double percentNumbers = calculateManualPercentByClass(Number.class);
        Assert.assertEquals(percentNumbers,
                map.percentKeyDataType(DataType.NUMBER), 0);
    }

    @Test
    public void testLink() {
        double otherCount = Random.getScaleCount();
        for (int i = 0; i < otherCount; i++) {
            map.insert(Random.getObject(), Random.getLong());
        }
        boolean addLink = Random.getBoolean();
        if(addLink) {
            map.insert(Link.to(Random.getLong()), Random.getLong());
        }
        Assert.assertEquals(addLink, map.containsDataType(DataType.LINK));
        Assert.assertEquals(addLink ? 1d / (otherCount + 1) : 0,
                map.percentKeyDataType(DataType.LINK), 0);
    }

    private double calculateManualPercentByClass(Class<?> clazz) {
        double count = Random.getScaleCount();
        double dataTypeCount = 0;
        for (int i = 0; i < count; i++) {
            Object key = Random.getObject();
            if(clazz.isAssignableFrom(key.getClass())) {
                dataTypeCount++;
            }
            map.insert(key, Random.getLong());
        }
        return dataTypeCount / count;
    }

}
