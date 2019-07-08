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
package com.cinchapi.concourse.data;

import java.util.Map;
import java.util.Set;

/**
 * An {@link Index} is an inverted data set in the form of a two-dimensional
 * mapping from key ({@link String}) and value ({@link String}) to records
 * {@link Long}.
 *
 * @author Jeff Nelson
 * @param V - the value type
 */
public interface Index<V> extends Map<String, Map<V, Set<Long>>> {}
