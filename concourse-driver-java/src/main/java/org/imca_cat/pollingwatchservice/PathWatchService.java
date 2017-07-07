/*
 * Copyright (c) 2014 J. Lewis Muir <jlmuir@imca-cat.org>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS
 * BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
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
  public WatchKey register(Path dir, WatchEvent.Kind<?>... kinds) throws IOException;

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
  public WatchKey register(Path dir, WatchEvent.Kind<?>[] kinds, WatchEvent.Modifier... modifiers)
      throws IOException;
}
