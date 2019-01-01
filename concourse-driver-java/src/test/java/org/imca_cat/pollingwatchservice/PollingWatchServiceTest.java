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
            StandardWatchEventKinds.ENTRY_CREATE,
            StandardWatchEventKinds.ENTRY_DELETE,
            StandardWatchEventKinds.ENTRY_MODIFY };

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
    public void testPoll_fileCreated()
            throws InterruptedException, IOException {
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
        assertEquals(a.getFileName(), (Path) event.context());
    }

    @Test
    public void testPoll_fileCreated_noMetadata()
            throws InterruptedException, IOException {
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
        assertEquals(a.getFileName(), (Path) event.context());
    }

    @Test
    public void testPoll_fileModified()
            throws InterruptedException, IOException {
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
        Files.setLastModifiedTime(a, FileTime
                .fromMillis(Files.getLastModifiedTime(a).toMillis() + 1L));
        k = service.take();
        events = k.pollEvents();
        assertEquals(1, events.size());
        event = events.get(0);
        assertEquals(StandardWatchEventKinds.ENTRY_MODIFY, event.kind());
        assertEquals(1, event.count());
        assertEquals(a.getFileName(), (Path) event.context());
    }

    @Test
    public void testPoll_fileModified_noMetadata()
            throws InterruptedException, IOException {
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
        Files.setLastModifiedTime(a, FileTime
                .fromMillis(Files.getLastModifiedTime(a).toMillis() + 1L));
        k = service.poll(1L, TimeUnit.SECONDS);
        assertNull(k);
    }

    @Test
    public void testPoll_fileDeleted()
            throws InterruptedException, IOException {
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
        assertEquals(a.getFileName(), (Path) event.context());
    }

    @Test
    public void testPoll_fileDeleted_noMetadata()
            throws InterruptedException, IOException {
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
        assertEquals(a.getFileName(), (Path) event.context());
    }

    private static void closeSilently(AutoCloseable c) {
        if(c == null)
            return;
        boolean interrupted = false;
        try {
            for (;;) {
                try {
                    c.close();
                    break;
                }
                catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        }
        catch (InterruptedIOException e) {
            Thread.currentThread().interrupt();
        }
        catch (ThreadDeath td) {
            throw td;
        }
        catch (Throwable t) {
            /* IGNORED */
        }
        finally {
            if(interrupted)
                Thread.currentThread().interrupt();
        }
    }
}
