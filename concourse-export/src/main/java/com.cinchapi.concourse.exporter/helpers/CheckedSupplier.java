package com.cinchapi.concourse.exporter.helpers;

@FunctionalInterface
public interface CheckedSupplier<T> {
    T get() throws Exception;
}
