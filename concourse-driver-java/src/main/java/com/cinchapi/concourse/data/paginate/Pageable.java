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
package com.cinchapi.concourse.data.paginate;

import javax.annotation.concurrent.NotThreadSafe;

import com.cinchapi.concourse.lang.paginate.Page;

/**
 * A {@link Pageable} object can {@link #paginate(Page)} itself and limit the
 * view of its data in accordance with a specific {@link Page}.
 *
 * @author Jeff Nelson
 */
@NotThreadSafe
public interface Pageable {

    /**
     * Paginate this object and limit the data that is accessible to only that
     * which belongs on the {@code page}.
     * <p>
     * After this method is called, the accessible data is limited in the same
     * manner as it would be as if the
     * {@link java.util.stream.Stream#limit(int)} and
     * {@link java.util.stream.Stream#skip(int)} methods were called on a
     * collection, but all the data remains present such that another call to
     * this method with a different {@link Page} can make data from the original
     * dataset, albeit in a different "page window", accessible.
     * </p>
     * 
     * @param page
     */
    public void paginate(Page page);

}
