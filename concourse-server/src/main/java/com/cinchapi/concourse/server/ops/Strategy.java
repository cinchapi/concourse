/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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
    private final Command command;

    /**
     * The {@link Store} that will handle the request.
     */
    private final Store store;

    /**
     * Construct a new instance.
     * 
     * @param command
     * @param store
     */
    public Strategy(Command command, Store store) {
        this.command = command;
        this.store = store;
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
        boolean isHistoricalOperation = command.operationTimestamp() != null;
        boolean isWideOperation = command.operationKeys().isEmpty()
                && !command.operation().startsWith("find");
        boolean isConditionKey = command.conditionKeys().contains(key);
        boolean isOrderKey = command.orderKeys().contains(key);
        boolean isKeyRequiringTimestampRetrieval = command
                .keysRequiringTimestampRetrieval().containsKey(key)
                || (isOrderKey && isHistoricalOperation);
        Source source;
        if((isConditionKey || isOrderKey) && !isKeyRequiringTimestampRetrieval
                && command.operationRecords().size() != 1) {
            // The IndexRecord must be loaded to evaluate the condition, so
            // leverage it to gather the values for key/record
            source = Source.INDEX;
        }
        else if(isWideOperation) {
            // The entire record is involved in the operation, so force the full
            // TableRecord to be loaded.
            source = Source.RECORD;
        }
        else if(memory.contains(record)) {
            source = Source.RECORD;
        }
        else if(memory.contains(key, record)) {
            source = Source.FIELD;
        }
        else {
            source = command.operationKeys().size() > 1 ? Source.RECORD
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
