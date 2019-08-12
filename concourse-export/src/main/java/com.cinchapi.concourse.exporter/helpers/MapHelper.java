package com.cinchapi.concourse.exporter.helpers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.*;
import java.util.stream.Collectors;

public class MapHelper {
    public static <T, Q, R> List<R> map(Map<T, Q> xs,
            BiFunction<? super T, ? super Q, ? extends R> f) {
        return xs.entrySet().stream()
                .map(e -> f.apply(e.getKey(), e.getValue()))
                .collect(Collectors.toList());
    }

    public static <T, Q> List<Map.Entry<T, Q>> toList(Map<T, Q> xs) {
        return new ArrayList<>(xs.entrySet());
    }

    public static <T, Q> Map<T, Q> toMap(List<Map.Entry<T, Q>> xs) {
        return xs.stream().collect(Collectors.toMap(
                Map.Entry::getKey, Map.Entry::getValue));
    }

    public static <T, Q> Map<T, Q> filter(Map<T, Q> xs, BiPredicate<T, Q> f) {
        return xs.entrySet().stream()
                .filter(e -> f.test(e.getKey(), e.getValue())).collect(
                        Collectors.toMap(
                                Map.Entry::getKey, Map.Entry::getValue));
    }
}


