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
    if (this == o) return true;
    if (!(o instanceof PollingWatchEvent<?>)) return false;
    PollingWatchEvent<?> e = (PollingWatchEvent<?>)o;
    if (!(kind.equals(e.kind))) return false;
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
    return new PollingWatchEvent<Path>(StandardWatchEventKinds.ENTRY_CREATE, p);
  }

  public static PollingWatchEvent<Path> modify(Path p) {
    return new PollingWatchEvent<Path>(StandardWatchEventKinds.ENTRY_MODIFY, p);
  }

  public static PollingWatchEvent<Path> delete(Path p) {
    return new PollingWatchEvent<Path>(StandardWatchEventKinds.ENTRY_DELETE, p);
  }

  public static PollingWatchEvent<?> overflow(Object o) {
    return new PollingWatchEvent<Object>(StandardWatchEventKinds.OVERFLOW, o);
  }
}
