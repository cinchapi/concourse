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
package com.cinchapi.concourse.server.storage;

import com.cinchapi.concourse.server.concurrent.Token;

/**
 * An object that notifies listeners about a version change so that they can
 * respond appropriately.
 * 
 * @author Jeff Nelson
 */
public interface VersionChangeNotifier {

    /**
     * Add {@code listener} to the list to be notified about version changes for
     * {@code token}.
     * 
     * @param token
     * @param listener
     */
    public void addVersionChangeListener(Token token,
            VersionChangeListener listener);

    /**
     * Remove {@code listener} from the list to be notified about version
     * changes for {@code token}.
     * 
     * @param token
     * @param listener
     */
    public void removeVersionChangeListener(Token token,
            VersionChangeListener listener);

    /**
     * Notify all relevant listeners about a version change for {@code token}.
     * 
     * @param token
     */
    public void notifyVersionChange(Token token);

}
