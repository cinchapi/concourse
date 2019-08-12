package com.cinchapi.concourse.exporter;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.function.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Helper {
    public static <T, Q> Map.Entry<T, Q> tuple(T t, Q q) {
        return new AbstractMap.SimpleImmutableEntry<>(t, q);
    }

    public static <T, Q> List<Map.Entry<T, Q>> zip(List<T> xs, List<Q> ys) {
        return IntStream.range(0, xs.size())
                .mapToObj(i -> tuple(xs.get(i), ys.get(i)))
                .collect(Collectors.toList());
    }

    public static <T, Q> List<Q> map(
            List<T> xs,
            Function<? super T, ? extends Q> f
    ) {
        return xs.stream().map(f).collect(Collectors.toList());
    }

    public static <T, Q, R> Iterable<R> map(
            Map<T, Q> xs,
            BiFunction<? super T, ? super Q, ? extends R> f) {
        return xs.entrySet().stream()
                .map(e -> f.apply(e.getKey(), e.getValue()))
                .collect(Collectors.toList());
    }

    /*
     * NOTE: Cannot overload with other maps due to Java's generics being
     * erased at runtime with no ability to reify them.
     */
    public static <T, Q, A, B> Map<A, B> mapToMap(
            Map<T, Q> xs,
            BiFunction<? super T, ? super Q, ? extends Map.Entry<A, B>> f
    ) {
        return xs.entrySet().stream()
                .map(e -> f.apply(e.getKey(), e.getValue()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey, Map.Entry::getValue));
    }

    public static <T, Q> Map<T, Q> filter(
            Map<T, Q> xs,
            BiPredicate<T, Q> f
    ) {
        return xs.entrySet().stream()
                .filter(e -> f.test(e.getKey(), e.getValue()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey, Map.Entry::getValue));
    }

    public static <T, Q> Q mapOrNull(T t, Function<T, Q> f) {
        return t != null ? f.apply(t) : null;
    }

    public static <T> T orElse(T t, T t2) {
        return t != null ? t : t2;
    }

    public static OutputStream createFileOutputStreamOrNull(String fileName) {

        try {
            final Path path = createFileOrNull(fileName);
            return path != null ? Files.newOutputStream(path) : null;
        } catch (IOException e) {
            return null;
        }
    }

    public static Path createFileOrNull(String fileName) {
        final Path path = getPathOrNull(fileName);
        return path != null ? createFileOrNull(path) : null;
    }

    public static Path createFileOrNull(Path path) {
        return tryOrNull(() -> Files.createFile(path));
    }

    public static Path getPathOrNull(String path) {
        return tryOrNull(() -> Paths.get(path));
    }

    public static <T> T tryOrNull(CheckedSupplier<T> f) {
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

    @FunctionalInterface
    interface CheckedFunction<T, Q> {
        Q apply(T t) throws Exception;
    }
}
