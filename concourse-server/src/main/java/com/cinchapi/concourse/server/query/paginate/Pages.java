/*
 * Copyright (c) 2013-2024 Cinchapi Inc.
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
package com.cinchapi.concourse.server.query.paginate;

import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.thrift.JavaThriftBridge;
import com.cinchapi.concourse.thrift.TPage;

/**
 * Utilities for {@link Page Pages}.
 *
 * @author Jeff Nelson
 */
public final class Pages {

    /**
     * Convert the {@link TPage} to its analogous {@link Page}.
     * 
     * @param tpage
     * @return the corresponding {@link Page}
     */
    public static Page from(TPage tpage) {
        return tpage == null ? Page.none() : JavaThriftBridge.convert(tpage);
    }

    private Pages() {/* no-init */}

}
