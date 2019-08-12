package com.cinchapi.concourse.exporter.helpers;

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

    /*
     * Just a combination of `map` and `$try` above to reduce the overhead
     * of using those two functions together. Instead of:
     * Null.map(object, o -> Null.$try(() -> function(o)));
     * it's:
     * Null.mapAndTry(object, o -> function(o))
     * Ideally, we could construct a DSL that would allow for:
     * object.map(o -> tryOrNull { function(o) })
     * but Java doesn't have extension functions or top level functions.
     */
    public static <T, Q> Q mapAndTry(T t, CheckedFunction<T, Q> f) {
        return t != null ? $try(() -> f.apply(t)) : null;
    }

    @FunctionalInterface
    interface CheckedSupplier<T> {
        T get() throws Exception;
    }

    @FunctionalInterface
    interface CheckedFunction<T, Q> {
        Q apply(T t) throws Exception;
    }
}
