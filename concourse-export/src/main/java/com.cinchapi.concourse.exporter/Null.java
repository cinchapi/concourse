package com.cinchapi.concourse.exporter;

import java.util.function.Function;

public class Null {
    public static <T, Q> Q map(T t, Function<T, Q> f) {
        return t != null ? f.apply(t) : null;
    }
    public static <T> T orElse(T t, T t2) {
        return t != null ? t : t2;
    }
    public static <T> T $try(CheckedSupplier<T> f) {
        try {
            return f.get();
        } catch (Exception e) {
            return null;
        }
    }

    @FunctionalInterface
    interface CheckedSupplier<T> {
        T get() throws Exception;
    }
}
