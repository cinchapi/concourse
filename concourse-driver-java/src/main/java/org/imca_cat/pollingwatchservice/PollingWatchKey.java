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
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.Watchable;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A {@code WatchKey} implementation for {@link PollingWatchService}.
 */
class PollingWatchKey implements WatchKey {
    private final PollingWatchService service;
    private final Path path;
    private final List<WatchEvent.Kind<?>> kinds;
    private final List<WatchEvent.Modifier> modifiers;
    private Map<Path, BasicFileAttributes> entries;
    private final List<PollingWatchEvent<?>> events;
    private final List<PollingWatchEvent<?>> eventsPending;
    private PollingWatchKeyState state;

    public PollingWatchKey(PollingWatchService service, Path path) {
        this(service, path, new WatchEvent.Kind<?>[0],
                new WatchEvent.Modifier[0]);
    }

    public PollingWatchKey(PollingWatchService service, Path path,
            WatchEvent.Kind<?>[] kinds, WatchEvent.Modifier[] modifiers) {
        this(service, path, kinds, modifiers, null);
    }

    public PollingWatchKey(PollingWatchService service, Path path,
            WatchEvent.Kind<?>[] kinds, WatchEvent.Modifier[] modifiers,
            Map<Path, BasicFileAttributes> entries) {
        super();
        this.service = service;
        this.path = path;
        this.kinds = new ArrayList<>(Arrays.asList(kinds));
        this.modifiers = new ArrayList<>(Arrays.asList(modifiers));
        this.entries = entries;
        events = new ArrayList<>();
        eventsPending = new ArrayList<>();
        state = PollingWatchKeyState.READY;
    }

    public synchronized boolean hasKindAndModifier(WatchEvent.Kind<?> k,
            WatchEvent.Modifier m) {
        return hasKind(k) && hasModifier(m);
    }

    public synchronized void kindsAndModifiers(WatchEvent.Kind<?>[] kinds,
            WatchEvent.Modifier[] modifiers) {
        this.kinds.clear();
        this.kinds.addAll(Arrays.asList(kinds));
        this.modifiers.clear();
        this.modifiers.addAll(Arrays.asList(modifiers));
    }

    public synchronized boolean hasKind(WatchEvent.Kind<?> k) {
        return kinds.contains(k);
    }

    public synchronized boolean hasModifier(WatchEvent.Modifier m) {
        return modifiers.contains(m);
    }

    /**
     * Returns a read-only map of entries.
     */
    public synchronized Map<Path, BasicFileAttributes> entries() {
        return Collections.unmodifiableMap(entries);
    }

    /**
     * Sets entries. The entries map must not be modified externally after being
     * set.
     */
    public synchronized void entries(Map<Path, BasicFileAttributes> entries) {
        this.entries = entries;
    }

    public synchronized boolean isReady() {
        return state.equals(PollingWatchKeyState.READY);
    }

    public synchronized boolean isSignaled() {
        return state.equals(PollingWatchKeyState.SIGNALED);
    }

    public synchronized boolean isInvalid() {
        return state.equals(PollingWatchKeyState.INVALID);
    }

    @Override
    public synchronized boolean isValid() {
        return !isInvalid();
    }

    @Override
    public synchronized List<WatchEvent<?>> pollEvents() {
        List<WatchEvent<?>> result = new ArrayList<WatchEvent<?>>(events);
        events.clear();
        return result;
    }

    public synchronized void signal() {
        if(isInvalid())
            return;
        if(isSignaled())
            return;
        state = PollingWatchKeyState.SIGNALED;
    }

    public synchronized void enqueue() {
        if(isSignaled())
            return;
        if(isReady())
            signal();
        service.enqueue(this);
    }

    public synchronized void enqueueIfReadyAndHasEvents() {
        if(!isReady())
            return;
        if(events.isEmpty())
            return;
        signal();
        service.enqueue(this);
    }

    @Override
    public synchronized boolean reset() {
        if(isInvalid())
            return false;
        if(isReady())
            return true;
        events.addAll(eventsPending);
        eventsPending.clear();
        state = PollingWatchKeyState.READY;
        if(!events.isEmpty())
            enqueue();
        return true;
    }

    @Override
    public synchronized void cancel() {
        if(isInvalid())
            return;
        state = PollingWatchKeyState.INVALID;
    }

    @Override
    public Watchable watchable() {
        return path;
    }

    public Path watchablePath() {
        return path;
    }

    public synchronized void addEvent(PollingWatchEvent<?> e) {
        List<PollingWatchEvent<?>> l = isReady() ? events : eventsPending;
        if(!l.isEmpty() && (e.isModify() || e.isOverflow())) {
            PollingWatchEvent<?> last = l.get(l.size() - 1);
            if(last.kind().equals(e.kind())) {
                last.incrementCount();
            }
            else {
                l.add(e);
            }
        }
        else {
            l.add(e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if(this == o)
            return true;
        if(!(o instanceof PollingWatchKey))
            return false;
        return path.equals(((PollingWatchKey) o).path);
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + path.hashCode();
        return result;
    }

    @Override
    public synchronized String toString() {
        return String.format("%s (state=%s, kinds=%s, modifiers=%s, entries)",
                path, state, kinds, modifiers, entries);
    }
}
