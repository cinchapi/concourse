/*
 * Copyright (c) 2013-2016 Cinchapi Inc.
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
package com.cinchapi.concourse.server.storage;

import com.cinchapi.concourse.plugin.Storage;

/**
 * <p>
 * A {@link Store} is a revisioning service that defines primitive operations to
 * read data from both current and previous states.
 * </p>
 * <p>
 * A {@code Store} can acquire data in one of two ways: directly if it is a
 * {@link Limbo} or <em>eventually</em> if it is a {@link PermanentStore}.
 * </p>
 * <p>
 * In general, a {@code Limbo} and {@code PermanentStore} work together in a
 * {@link BufferedStore} to improve write performance by immediately committing
 * writes into a durable buffer before batch indexing them in the background.
 * </p>
 * 
 * @author Jeff Nelson
 */
public interface Store extends Storage {

    /**
     * Start the service.
     */
    public void start();

    /**
     * Stop the service.
     */
    public void stop();

}
