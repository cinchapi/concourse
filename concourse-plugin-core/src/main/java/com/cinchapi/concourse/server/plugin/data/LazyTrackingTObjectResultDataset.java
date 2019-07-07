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
package com.cinchapi.concourse.server.plugin.data;

import java.util.Set;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import com.cinchapi.concourse.data.sort.SortableTable;
import com.cinchapi.concourse.data.sort.Sorter;
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.thrift.TObject;

/**
 * An analog to {@link TObjectResultDataset} that lazily tracks data in
 * accordance with the specification of {@link LazyTackingResultDataset}.
 *
 * @author Jeff Nelson
 */
public class LazyTrackingTObjectResultDataset
        extends LazyTrackingResultDataset<TObject>
        implements SortableTable<Set<TObject>> {
    
    /**
     * The last {@link Page} specified in the {@link #paginate(Page)} method.
     */
    @Nullable
    private Page page;

    @Override
    public void sort(Sorter<Set<TObject>> sorter) {
        data = sorter.sort(data);
        if(tracking != null) {
            tracking();
        }
    }

    @Override
    public void sort(Sorter<Set<TObject>> sorter, long at) {
        data = sorter.sort(data, at);
        if(tracking != null) {
            tracking();
        }
    }

    @Override
    protected Supplier<ResultDataset<TObject>> supplier() {
        return TObjectResultDataset::new;
    }
}
