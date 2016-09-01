package com.cinchapi.concourse.plugin.data;

import java.util.Map;
import java.util.Set;

public abstract class AbstractDataset<V> extends Dataset<Long, String, V> {

    private static final long serialVersionUID = -1612162452019105991L;

    @Override
    protected Map<V, Set<Long>> createInvertedMultimap() {
        return TrackingLinkedHashMultimap.create();
    }

}
