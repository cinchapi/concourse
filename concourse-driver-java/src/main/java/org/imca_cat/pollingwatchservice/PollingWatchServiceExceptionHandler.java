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
package org.imca_cat.pollingwatchservice;

/**
 * An exception handler interface for {@link PollingWatchService}.
 */
public interface PollingWatchServiceExceptionHandler {
    /**
     * Called when an exception has been caught but not handled. May be called
     * from multiple concurrent threads.
     *
     * @param t exception that was caught
     */
    void exception(Throwable t);
}
