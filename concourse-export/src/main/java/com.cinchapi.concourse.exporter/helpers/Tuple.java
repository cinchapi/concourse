package com.cinchapi.concourse.exporter.helpers;

import java.util.Map;

public final class Tuple<T, Q> implements Map.Entry<T, Q> {
    final T _1;
    final Q _2;

    public Tuple(T t, Q q) {
        _1 = t;
        _2 = q;
    }

    @Override public T getKey() {
        return _1;
    }

    @Override public Q getValue() {
        return _2;
    }

    @Override public Q setValue(Q value) {
        throw new RuntimeException("Tuples are immutable.");
    }
}
