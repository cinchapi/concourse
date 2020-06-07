/*
 * Copyright (c) 2013-2020 Cinchapi Inc.
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
import com.cinchapi.concourse.server.storage.Memory;
import com.cinchapi.concourse.server.storage.Store;

/**
 * A {@link Strategy} is a guided plan for interacting with a {@link Store} to
 * service a {@link ConcourseServer} {@link Command}.
 *
 * @author Jeff Nelson
 */
public class Strategy {

    /**
     * The {@link Command} being serviced.
     */
    private final Command request;

    /**
     * The {@link Store} that will handle the request.
     */
    private final Store store;

    /**
     * A boolean that tracks if the {@link #store} is {@link Gatherable}.
     */
    private final boolean gatherable;

    /**
     * Construct a new instance.
     * 
     * @param request
     * @param store
     */
    public Strategy(Command request, Store store) {
        this.request = request;
        this.store = store;
        this.gatherable = store instanceof Gatherable;
    }

    /**
     * Return the {@link Source} that this {@link Strategy} recommends for
     * looking up the {@code key} in {@code record}.
     * 
     * @param key
     * @param record
     * @return the {@link Source} to use for looking up {@code key} in
     *         {@code record}
     */
    public Source source(String key, long record) {
        Memory memory = store.memory();
        // TODO: The notion of a "wide" operation should be extended to case
        // when the majority (or significant number) of, but not all keys are
        // involved with an operation.
        boolean isWideOperation = request.operationKeys().isEmpty()
                && !request.operation().startsWith("find");
        boolean isConditionKey = request.conditionKeys().contains(key);
        boolean isOrderKey = request.orderKeys().contains(key);
        Source source;
        if((isConditionKey || isOrderKey)
                && request.operationRecords().size() != 1 && gatherable) {
            // The SecondaryRecord must be loaded to evaluate the condition, so
            // leverage it to gather the values for key/record
            source = Source.INDEX;
        }
        else if(isWideOperation) {
            // The entire record is involved in the operation, so force the full
            // PrimaryRecord to be loaded.
            source = Source.RECORD;
        }
        else if(memory.contains(record)) {
            source = Source.RECORD;
        }
        else if(memory.contains(key, record)) {
            source = Source.FIELD;
        }
        else {
            source = request.operationKeys().size() > 1 ? Source.RECORD
                    : Source.FIELD;
        }
        return source;
    }

    /**
     * The various structures where a data lookup can be performed.
     *
     * @author Jeff Nelson
     */
    public enum Source {
        RECORD, FIELD, INDEX
    }

}
