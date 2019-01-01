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
package org.imca_cat.pollingwatchservice;

import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;

/**
 * A {@code WatchEvent} implementation for {@link PollingWatchService}.
 */
class PollingWatchEvent<T> implements WatchEvent<T> {
    private final WatchEvent.Kind<T> kind;
    private final T context;
    private int count;

    public PollingWatchEvent(WatchEvent.Kind<T> kind, T context) {
        super();
        this.kind = kind;
        this.context = context;
        count = 1;
    }

    @Override
    public WatchEvent.Kind<T> kind() {
        return kind;
    }

    public boolean isCreate() {
        return kind.equals(StandardWatchEventKinds.ENTRY_CREATE);
    }

    public boolean isModify() {
        return kind.equals(StandardWatchEventKinds.ENTRY_MODIFY);
    }

    public boolean isDelete() {
        return kind.equals(StandardWatchEventKinds.ENTRY_DELETE);
    }

    public boolean isOverflow() {
        return kind.equals(StandardWatchEventKinds.OVERFLOW);
    }

    @Override
    public int count() {
        return count;
    }

    public void incrementCount() {
        count++;
    }

    @Override
    public T context() {
        return context;
    }

    @Override
    public boolean equals(Object o) {
        if(this == o)
            return true;
        if(!(o instanceof PollingWatchEvent<?>))
            return false;
        PollingWatchEvent<?> e = (PollingWatchEvent<?>) o;
        if(!(kind.equals(e.kind)))
            return false;
        return context.equals(e.context);
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + kind.hashCode();
        result = 31 * result + context.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return String.format("%s: %s", kind, context);
    }

    public static PollingWatchEvent<Path> create(Path p) {
        return new PollingWatchEvent<Path>(StandardWatchEventKinds.ENTRY_CREATE,
                p);
    }

    public static PollingWatchEvent<Path> modify(Path p) {
        return new PollingWatchEvent<Path>(StandardWatchEventKinds.ENTRY_MODIFY,
                p);
    }

    public static PollingWatchEvent<Path> delete(Path p) {
        return new PollingWatchEvent<Path>(StandardWatchEventKinds.ENTRY_DELETE,
                p);
    }

    public static PollingWatchEvent<?> overflow(Object o) {
        return new PollingWatchEvent<Object>(StandardWatchEventKinds.OVERFLOW,
                o);
    }
}
