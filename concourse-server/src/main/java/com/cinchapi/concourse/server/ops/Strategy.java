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
package com.cinchapi.concourse.server.ops;

import com.cinchapi.concourse.server.ConcourseServer;
import com.cinchapi.concourse.server.storage.Gatherable;
import com.cinchapi.concourse.server.storage.Store;

/**
 * A {@link Strategy} is a guided plan for interacting with a {@link Store} to
 * service a {@link ConcourseServer} {@link Request}.
 *
 * @author Jeff Nelson
 */
public class Strategy {

    /**
     * The {@link Request} being serviced.
     */
    private final Request request;

    /**
     * The {@link Store} that will handle the request.
     */
    private final Store store;

    /**
     * Construct a new instance.
     * 
     * @param request
     * @param store
     */
    public Strategy(Request request, Store store) {
        this.request = request;
        this.store = store;
    }

    /**
     * Return {@code true} if it is recommended that the
     * {@link Gatherable#gather(String, long) gather} method be used instead of
     * the {@link Store#select(String, long) select} method to fetch values for
     * {@code key} across {@code records} in service of the
     * {@link #operation()}.
     * <p>
     * If the {@link #store} is not {@link Gatherable}, this method returns
     * {@ocode false}.
     * </p>
     * 
     * @param key
     * @param records
     * @return a boolean that indicates whether values should be retrieved by
     *         gathering instead of selecting
     */
    public boolean shouldGather(String key, long... records) {
        if(store instanceof Gatherable) {
            // TODO: get store state
            if(request.conditionKeys().contains(key)) {
                // TODO: what if short circuiting happens?
                // This would be true if the secondary record was consulted...
                return true;
            }
            else {
                return false;
            }
        }
        else {
            return false;
        }

    }

}
