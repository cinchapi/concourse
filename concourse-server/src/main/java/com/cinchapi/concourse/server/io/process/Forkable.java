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
package com.cinchapi.concourse.server.io.process;

import java.io.Serializable;
import java.util.concurrent.Callable;

import com.cinchapi.common.reflect.Reflection;
import com.google.common.reflect.TypeToken;

/**
 * A {@link Callable} that is, itself, {@link Serializable} and also return a
 * value that is {@link Serializable} from the {@link #call()} method.
 * 
 * @author Jeff Nelson
 */
public abstract class Forkable<T extends Serializable> implements
        Serializable,
        Callable<T> {

    private static final long serialVersionUID = -1814334322385050811L;

    /**
     * An internal {@link TypeToken} that helps to determine the generic type
     * for this object.
     */
    @SuppressWarnings("serial")
    private final TypeToken<T> typeToken = new TypeToken<T>(getClass()) {};

    /**
     * The value returned from {@link #getReturnType()}.
     */
    private final Class<T> type = Reflection.getClassCasted(typeToken.getType()
            .toString().split("<")[0]);

    /**
     * Get the {@link Class} object for the generic return type.
     * 
     * @return a return {@link Class type}
     */
    public Class<T> getReturnType() {
        return type;
    }

}
