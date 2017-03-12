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
package com.cinchapi.concourse.util;

import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Factory methods for creating {@link ThreadFactory} objects. OOP design
 * principals for the win (note sarcasm).
 * 
 * @author Jeff Nelson
 */
public class ThreadFactories {

    /**
     * Return a {@link ThreadFactory} that uses the specified {@code nameFormat}
     * for new threads.
     * 
     * @param nameFormat the name format for new threads
     * @return a {@link ThreadFactory} configured to name new threads using the
     *         {@code nameFormat} and all other default options
     */
    public static ThreadFactory namingThreadFactory(String nameFormat) {
        return new ThreadFactoryBuilder().setNameFormat(nameFormat).build();
    }

    /**
     * Return a {@link ThreadFactory} that uses the specified {@code nameFormat}
     * for new daemon threads.
     * 
     * @param nameFormat the name format for new threads
     * @return a {@link ThreadFactory} configured to name new threads using the
     *         {@code nameFormat} and all other default options
     */
    public static ThreadFactory namingDaemonThreadFactory(String nameFormat) {
        return new ThreadFactoryBuilder().setNameFormat(nameFormat)
                .setDaemon(true).build();
    }

}
