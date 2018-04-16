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
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;

/**
 * A service for watching {@code Path} objects for changes.
 * <p>
 * A {@code PathWatchService} is similar to a {@code WatchService} except that
 * a {@code PathWatchService} must be started, it only watches {@code Path}
 * instances (not {@code Watchable} instances), and instead of calling one of
 * the {@code register} methods on a {@code Watchable}, we call one of the
 * {@code register} methods on this interface.
 * <p>
 * To use this service, create an instance, register directories to watch via
 * the {@code register} methods, and start it via the {@code start} method.
 * Registering directories to watch can be performed before and after starting
 * the service. Retrieve changes via the {@code poll} or {@code take} methods,
 * and close the service when done via the {@code close} method.
 * <p>
 * Example:
 *
 * <pre>
 *     PathWatchService s = newPathWatchService();
 *     s.register(FileSystems.getDefault().getPath("/home/luke"),
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
public interface PathWatchService extends WatchService {
    /**
     * Starts this service.
     *
     * @throws ClosedWatchServiceException if closed
     * @throws IllegalStateException if already started
     */
    public void start();

    /**
     * Registers the specified directory, using the specified {@code WatchEvent}
     * kinds and no {@code WatchEvent} modifiers, with this watch service.
     * <p>
     * This method is intended to be similar in behavior to
     * {@link Path#register(WatchService, WatchEvent.Kind[])}.
     *
     * @param dir directory to watch; must not be a symbolic link
     * @param kinds events to register for
     *
     * @throws ClosedWatchServiceException if this watch service is closed
     * @throws NotDirectoryException if {@code dir} is not a directory
     * @throws IOException if an I/O error occurs
     */
    public WatchKey register(Path dir, WatchEvent.Kind<?>... kinds)
            throws IOException;

    /**
     * Registers the specified directory, using the specified {@code WatchEvent}
     * kinds and modifiers, with this watch service.
     * <p>
     * This method is intended to be similar in behavior to
     * {@link Path#register(WatchService, WatchEvent.Kind[], WatchEvent.Modifier[])}
     * .
     *
     * @param dir directory to watch; must not be a symbolic link
     * @param kinds events to register for
     * @param modifiers modifiers modifying how {@code dir} is registered
     *
     * @throws ClosedWatchServiceException if this watch service is closed
     * @throws NotDirectoryException if {@code dir} is not a directory
     * @throws IOException if an I/O error occurs
     */
    public WatchKey register(Path dir, WatchEvent.Kind<?>[] kinds,
            WatchEvent.Modifier... modifiers) throws IOException;
}
