/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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
package com.cinchapi.concourse.util;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.google.common.io.Files;

/**
 * Unit tests for {@link ConcourseCodebase}.
 * 
 * @author Jeff Nelson
 */
public class ConcourseCodebaseTest {

    private static final String ORIGINAL_USER_DIR = System
            .getProperty("user.dir");

    @Rule
    public final TestWatcher __watcher = new TestWatcher() {

        @Override
        protected void finished(Description description) {
            System.setProperty("user.dir", ORIGINAL_USER_DIR);
            ConcourseCodebase.INSTANCE = null;
            Path cache = Paths.get(System.getProperty("user.home"),
                    ConcourseCodebase.REPO_CACHE_FILE);
            cache.toFile().delete();
        }

    };

    @Test
    public void testCloneFromGithubFromWithinRepo() {
        // Assumes that unit test is running from the concourse repo
        ConcourseCodebase codebase = ConcourseCodebase.cloneFromGithub();
        Path codepath = Paths.get(codebase.getPath());
        Path repopath = Paths.get(System.getProperty("user.dir"));
        Assert.assertTrue(isDirectoryOrSubdirectoryOf(codepath, repopath));
    }

    @Test
    public void testCloneFromGithubFromOutsideRepo() {
        System.setProperty("user.dir", Files.createTempDir().toString());
        ConcourseCodebase codebase = ConcourseCodebase.cloneFromGithub();
        Path codepath = Paths.get(codebase.getPath());
        Path repopath = Paths.get(System.getProperty("user.dir"));
        Assert.assertFalse(isDirectoryOrSubdirectoryOf(codepath, repopath));
        Path cache = Paths.get(ConcourseCodebase.REPO_CACHE_FILE);
        Assert.assertTrue(cache.toFile().exists());
        Assert.assertTrue(isDirectoryOrSubdirectoryOf(Paths.get(FileOps
                .readLines(cache.toString()).iterator().next()), codepath));
        cache.toFile().delete();
    }

    @Test
    public void testCloneFromGithubOutsideRepoCached() {
        System.setProperty("user.dir", Files.createTempDir().toString());
        ConcourseCodebase codebase = ConcourseCodebase.cloneFromGithub();
        ConcourseCodebase.INSTANCE = null; // remove the local process cache
        codebase = ConcourseCodebase.cloneFromGithub();
        Path codepath = Paths.get(codebase.getPath());
        Path repopath = Paths.get(System.getProperty("user.dir"));
        Assert.assertFalse(isDirectoryOrSubdirectoryOf(codepath, repopath));
        Path cache = Paths.get(ConcourseCodebase.REPO_CACHE_FILE);
        Assert.assertTrue(cache.toFile().exists());
        Assert.assertTrue(isDirectoryOrSubdirectoryOf(Paths.get(FileOps
                .readLines(cache.toString()).iterator().next()), codepath));
        cache.toFile().delete();
    }

    @Test
    public void testCloneFromGithubDetectStaleCache() {
        System.setProperty("user.dir", Files.createTempDir().toString());
        ConcourseCodebase codebase = ConcourseCodebase.cloneFromGithub();
        Path path = Paths
                .get(FileOps
                        .readLines(
                                Paths.get(ConcourseCodebase.REPO_CACHE_FILE)
                                        .toString()).iterator().next());
        path.toFile().delete();
        codebase = ConcourseCodebase.cloneFromGithub();
        Path codepath = Paths.get(codebase.getPath());
        Path repopath = Paths.get(System.getProperty("user.dir"));
        Assert.assertFalse(isDirectoryOrSubdirectoryOf(codepath, repopath));
        Path cache = Paths.get(ConcourseCodebase.REPO_CACHE_FILE);
        Assert.assertTrue(cache.toFile().exists());
        Assert.assertTrue(isDirectoryOrSubdirectoryOf(Paths.get(FileOps
                .readLines(cache.toString()).iterator().next()), codepath));
        cache.toFile().delete();
    }

    private static boolean isDirectoryOrSubdirectoryOf(Path parent, Path child) {
        parent = parent.normalize().toAbsolutePath();
        if(child != null) {
            child = child.normalize().toAbsolutePath();
        }
        if(child == null) {
            return false;
        }
        else if(parent.equals(child)) {
            return true;
        }
        else {
            return isDirectoryOrSubdirectoryOf(parent, child.getParent());
        }
    }

}
