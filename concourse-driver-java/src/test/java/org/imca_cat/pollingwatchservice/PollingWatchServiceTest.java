/*
 * Copyright (c) 2014, 2016 J. Lewis Muir <jlmuir@imca-cat.org>
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.attribute.FileTime;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.github.marschall.memoryfilesystem.MemoryFileSystemBuilder;

public class PollingWatchServiceTest {
  private static final WatchEvent.Kind<?>[] KINDS = new WatchEvent.Kind[] {
      StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_DELETE,
      StandardWatchEventKinds.ENTRY_MODIFY};

  private FileSystem fs;
  private PollingWatchService service;

  public PollingWatchServiceTest() {
    super();
    fs = null;
  }

  @Before
  public void startUp() throws IOException {
    fs = MemoryFileSystemBuilder.newLinux().build("test");
  }

  @After
  public void tearDown() throws Throwable {
    closeSilently(service);
    closeSilently(fs);
  }

  @Test
  public void testPoll_fileCreated() throws InterruptedException, IOException {
    Path tmp = fs.getPath("/tmp");
    Files.createDirectory(tmp);
    service = new PollingWatchService(1, 100L, TimeUnit.MILLISECONDS);
    service.register(tmp, KINDS);
    service.start();
    Path a = tmp.resolve("a.txt");
    Files.createFile(a);
    WatchKey k = service.take();
    List<WatchEvent<?>> events = k.pollEvents();
    assertEquals(1, events.size());
    WatchEvent<?> event = events.get(0);
    assertEquals(StandardWatchEventKinds.ENTRY_CREATE, event.kind());
    assertEquals(1, event.count());
    assertEquals(a.getFileName(), (Path)event.context());
  }

  @Test
  public void testPoll_fileCreated_noMetadata() throws InterruptedException, IOException {
    Path tmp = fs.getPath("/tmp");
    Files.createDirectory(tmp);
    service = new PollingWatchService(1, 100L, TimeUnit.MILLISECONDS);
    service.useFileMetadata(false);
    service.register(tmp, KINDS);
    service.start();
    Path a = tmp.resolve("a.txt");
    Files.createFile(a);
    WatchKey k = service.take();
    List<WatchEvent<?>> events = k.pollEvents();
    assertEquals(1, events.size());
    WatchEvent<?> event = events.get(0);
    assertEquals(StandardWatchEventKinds.ENTRY_CREATE, event.kind());
    assertEquals(1, event.count());
    assertEquals(a.getFileName(), (Path)event.context());
  }

  @Test
  public void testPoll_fileModified() throws InterruptedException, IOException {
    Path tmp = fs.getPath("/tmp");
    Files.createDirectory(tmp);
    service = new PollingWatchService(1, 100L, TimeUnit.MILLISECONDS);
    service.register(tmp, KINDS);
    service.start();
    Path a = tmp.resolve("a.txt");
    Files.createFile(a);
    WatchKey k = service.take();
    List<WatchEvent<?>> events = k.pollEvents();
    assertEquals(1, events.size());
    WatchEvent<?> event = events.get(0);
    assertEquals(StandardWatchEventKinds.ENTRY_CREATE, event.kind());
    k.reset();
    Files.setLastModifiedTime(a, FileTime.fromMillis(Files.getLastModifiedTime(a).toMillis() + 1L));
    k = service.take();
    events = k.pollEvents();
    assertEquals(1, events.size());
    event = events.get(0);
    assertEquals(StandardWatchEventKinds.ENTRY_MODIFY, event.kind());
    assertEquals(1, event.count());
    assertEquals(a.getFileName(), (Path)event.context());
  }

  @Test
  public void testPoll_fileModified_noMetadata() throws InterruptedException, IOException {
    Path tmp = fs.getPath("/tmp");
    Files.createDirectory(tmp);
    service = new PollingWatchService(1, 100L, TimeUnit.MILLISECONDS);
    service.useFileMetadata(false);
    service.register(tmp, KINDS);
    service.start();
    Path a = tmp.resolve("a.txt");
    Files.createFile(a);
    WatchKey k = service.take();
    List<WatchEvent<?>> events = k.pollEvents();
    assertEquals(1, events.size());
    WatchEvent<?> event = events.get(0);
    assertEquals(StandardWatchEventKinds.ENTRY_CREATE, event.kind());
    k.reset();
    Files.setLastModifiedTime(a, FileTime.fromMillis(Files.getLastModifiedTime(a).toMillis() + 1L));
    k = service.poll(1L, TimeUnit.SECONDS);
    assertNull(k);
  }

  @Test
  public void testPoll_fileDeleted() throws InterruptedException, IOException {
    Path tmp = fs.getPath("/tmp");
    Files.createDirectory(tmp);
    Path a = tmp.resolve("a.txt");
    Files.createFile(a);
    service = new PollingWatchService(1, 100L, TimeUnit.MILLISECONDS);
    service.register(tmp, KINDS);
    service.start();
    Files.delete(a);
    WatchKey k = service.take();
    List<WatchEvent<?>> events = k.pollEvents();
    assertEquals(1, events.size());
    WatchEvent<?> event = events.get(0);
    assertEquals(StandardWatchEventKinds.ENTRY_DELETE, event.kind());
    assertEquals(1, event.count());
    assertEquals(a.getFileName(), (Path)event.context());
  }

  @Test
  public void testPoll_fileDeleted_noMetadata() throws InterruptedException, IOException {
    Path tmp = fs.getPath("/tmp");
    Files.createDirectory(tmp);
    Path a = tmp.resolve("a.txt");
    Files.createFile(a);
    service = new PollingWatchService(1, 100L, TimeUnit.MILLISECONDS);
    service.useFileMetadata(false);
    service.register(tmp, KINDS);
    service.start();
    Files.delete(a);
    WatchKey k = service.take();
    List<WatchEvent<?>> events = k.pollEvents();
    assertEquals(1, events.size());
    WatchEvent<?> event = events.get(0);
    assertEquals(StandardWatchEventKinds.ENTRY_DELETE, event.kind());
    assertEquals(1, event.count());
    assertEquals(a.getFileName(), (Path)event.context());
  }

  private static void closeSilently(AutoCloseable c) {
    if (c == null) return;
    boolean interrupted = false;
    try {
      for (;;) {
        try {
          c.close();
          break;
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
    } catch (InterruptedIOException e) {
      Thread.currentThread().interrupt();
    } catch (ThreadDeath td) {
      throw td;
    } catch (Throwable t) {
      /* IGNORED */
    } finally {
      if (interrupted) Thread.currentThread().interrupt();
    }
  }
}
