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
package com.cinchapi.concourse.server.storage.db;

import com.cinchapi.common.base.AnyStrings;

/**
 * A {@link MalformedBlockException} is thrown when a Block does not contain all
 * of the components (e.g. stats, index, filter, etc) necessary to function
 * properly.
 * <p>
 * A "malformed" block is usually an indicator that the Database exited in the
 * middle of a syncing operation. Malformed blocks can usually be safely
 * discared.
 * </p>
 *
 * @author Jeff Nelson
 */
public class MalformedBlockException extends RuntimeException {

    private static final long serialVersionUID = -1721757690680045080L;

    /**
     * Construct a new instance.
     * 
     * @param id
     * @param directory
     * @param missing
     */
    public MalformedBlockException(String id, String directory,
            String... missing) {
        super(AnyStrings.format("Block {} in {} is missing {}", id, directory,
                String.join(", ", missing)));
    }

}
