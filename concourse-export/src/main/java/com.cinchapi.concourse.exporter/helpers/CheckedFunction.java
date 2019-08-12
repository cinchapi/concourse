package com.cinchapi.concourse.exporter.helpers;

@FunctionalInterface
interface CheckedFunction<T, Q> {
    Q apply(T t) throws Exception;
}