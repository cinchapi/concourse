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
  public WatchKey register(Path dir, Kind<?>[] kinds, Modifier... modifiers) throws IOException {
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
  public WatchKey poll(long timeout, TimeUnit unit) throws InterruptedException {
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
