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
package com.cinchapi.concourse.server.io;

import java.lang.reflect.Method;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.server.io.Byteable;
import com.cinchapi.concourse.server.io.Byteables;
import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.cinchapi.concourse.util.TestData;
import com.google.common.base.Throwables;

/**
 * Tests for {@link Byteable} objects.
 * 
 * @author Jeff Nelson
 */
public abstract class ByteableTest extends ConcourseBaseTest{

    /**
     * Return a random instance of the test class defined in
     * {@link #getTestClass()}. This method assumes that there is a method in
     * {@link TestData} that takes no arguments and returns the appropriate
     * type.
     * 
     * @return a random instance
     */
    protected Byteable getRandomTestInstance() {
        try {
            for (Method method : TestData.class.getMethods()) {
                if(method.getReturnType() == getTestClass()
                        && method.getParameterTypes().length == 0) {
                    return (Byteable) method.invoke(null);
                }
            }
            throw new IllegalStateException(
                    "There is no method in TestData that takes no parameters and returns a "
                            + getTestClass());
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }

    }

    /**
     * Return the test class
     * 
     * @return the test class
     */
    protected abstract Class<? extends Byteable> getTestClass();

    @Test
    public void testSerialization() {
        Byteable object = getRandomTestInstance();
        Assert.assertTrue(Byteables.readStatic(object.getBytes(),
                getTestClass()).equals(object));
    }

}
