package com.cinchapi.concourse.exporter.helpers;

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

    public static <T, Q> List<Q> map(List<T> xs,
            Function<? super T, ? extends Q> f) {
        return xs.stream().map(f).collect(Collectors.toList());
    }

    public static <T, Q, R> List<R> map(Map<T, Q> xs,
            BiFunction<? super T, ? super Q, ? extends R> f) {
        return xs.entrySet().stream()
                .map(e -> f.apply(e.getKey(), e.getValue()))
                .collect(Collectors.toList());
    }

    public static <T, Q> Map<T, Q> toMap(List<Map.Entry<T, Q>> xs) {
        return xs.stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static <T, Q> Map<T, Q> filter(Map<T, Q> xs, BiPredicate<T, Q> f) {
        return xs.entrySet().stream()
                .filter(e -> f.test(e.getKey(), e.getValue())).collect(
                        Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}


