package com.cinchapi.concourse.exporter.helpers;

public class NullHelper {
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
}
