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
package com.cinchapi.concourse.server.storage.db.search;

import com.cinchapi.concourse.server.model.Position;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.storage.Action;

/**
 * A {@link SearchIndex} stores indices to support fulltext search.
 *
 * @author Jeff Nelson
 */
public interface SearchIndex {

    /**
     * Insert an entry to support a search lookup for {@code term} as
     * {@code position} for {@code key}.
     * 
     * @param key
     * @param term
     * @param position
     * @param version
     * @param type
     */
    public void index(Text key, Text term, Position position, long version,
            Action type);

}
