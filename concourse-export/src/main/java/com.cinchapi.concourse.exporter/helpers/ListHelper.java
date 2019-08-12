/*
 * Copyright (c) 2013-2019 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
