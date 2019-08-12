package com.cinchapi.concourse.exporter.helpers;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ListHelper {
    public static <T, Q> List<Map.Entry<T, Q>> zip(List<T> xs, List<Q> ys) {
        return IntStream.range(0, xs.size())
                .mapToObj(i -> new Tuple<>(xs.get(i), ys.get(i)))
                .collect(Collectors.toList());
    }

    public static <T, Q> List<Q> map(List<T> xs,
            Function<? super T, ? extends Q> f) {
        return xs.stream().map(f).collect(Collectors.toList());
    }
}
