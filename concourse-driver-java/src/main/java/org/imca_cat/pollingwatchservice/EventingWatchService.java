/*
 * Copyright (c) 2013-2018 Cinchapi Inc.
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

import java.io.IOException;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.Path;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchEvent.Modifier;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.concurrent.TimeUnit;

/**
 * An <a href="http://en.wikipedia.org/wiki/Adapter_pattern">Adapter</a> that
 * adapts the {@code WatchService} interface to a {@link PathWatchService}.
 * Since the idea of a {@code WatchService} for a {@code FileSystem} is that it
 * maps onto the native event notification facility where available, this class
 * is considered to be an "eventing" implementation of the
 * {@code PathWatchService} interface.
 * <p>
 * Example:
 *
 * <pre>
 *     FileSystem fs = FileSystems.getDefault();
 *     PathWatchService s = new EventingWatchService(fs.newWatchService());
 *     s.register(fs.getPath("/home/luke"),
 *         StandardWatchEventKinds.ENTRY_CREATE,
 *         StandardWatchEventKinds.ENTRY_DELETE,
 *         StandardWatchEventKinds.ENTRY_MODIFY);
 *     s.start();
 *     for (;;) {
 *       WatchKey k = s.take();
 *       ...
 *     }
 * </pre>
 */
public class EventingWatchService implements PathWatchService {
    private final WatchService service;

    /**
     * Constructs an instance on the specified watch service.
     *
     * @param service the watch service to be adapted
     */
    public EventingWatchService(WatchService service) {
        super();
        this.service = service;
    }

    @Override
    public void start() {
        /* IGNORED */
    }

    /**
     * @throws ClosedWatchServiceException {@inheritDoc}
     */
    @Override
    public WatchKey register(Path dir, Kind<?>... kinds) throws IOException {
        return dir.register(service, kinds);
    }

    /**
     * @throws ClosedWatchServiceException {@inheritDoc}
     */
    @Override
    public WatchKey register(Path dir, Kind<?>[] kinds, Modifier... modifiers)
            throws IOException {
        return dir.register(service, kinds, modifiers);
    }

    @Override
    public void close() throws IOException {
        service.close();
    }

    /**
     * @throws ClosedWatchServiceException {@inheritDoc}
     */
    @Override
    public WatchKey poll() {
        return service.poll();
    }

    /**
     * @throws ClosedWatchServiceException {@inheritDoc}
     */
    @Override
    public WatchKey poll(long timeout, TimeUnit unit)
            throws InterruptedException {
        return service.poll(timeout, unit);
    }

    /**
     * @throws ClosedWatchServiceException {@inheritDoc}
     */
    @Override
    public WatchKey take() throws InterruptedException {
        return service.take();
    }
}
