/*
 * Copyright (c) 2013-2015 Cinchapi, Inc.
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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;

/**
 * A utility that can clone the concourse repo from Github.
 * 
 * @author Jeff Nelson
 */
public final class ConcourseRepoCloner {

    /**
     * Clone the repo and return the path to the local directory where it is
     * stored. Multiple attempts to clone the repo will resolve to the same
     * local directory for the lifetime of the JVM.
     * 
     * @return the directory where the repo is cloned
     */
    public static String cloneAndGetPath() {
        if(Strings.isNullOrEmpty(LOCAL_REPO_DIR)) {
            String dir = System.getProperty("user.dir");
            String odir = null;
            boolean checkParent = true;
            while (checkParent) {
                // Check to see if the process working directory is within a
                // clone/fork of the Concourse git repo. Keep checking parent
                // directories until we reach the root of the repo
                try {
                    Process originProc = new ProcessBuilder("git", "config",
                            "--get", "remote.origin.url").directory(
                            new File(dir)).start();
                    Process upstreamProc = new ProcessBuilder("git", "config",
                            "--get", "remote.upstream.url").directory(
                            new File(dir)).start();
                    List<String> originLines = Processes.getStdOut(originProc);
                    List<String> upstreamLines = Processes
                            .getStdOut(upstreamProc);
                    String originOut = !originLines.isEmpty() ? originLines
                            .get(0) : "";
                    String upstreamOut = !upstreamLines.isEmpty() ? upstreamLines
                            .get(0) : "";
                    if(VALID_REMOTE_URLS.contains(originOut)
                            || VALID_REMOTE_URLS.contains(upstreamOut)) {
                        checkParent = true;
                        odir = dir;
                        dir = dir + "/..";
                    }
                    else {
                        dir = odir;
                        checkParent = false;
                    }
                }
                catch (Exception e) {
                    dir = odir;
                    checkParent = false;
                }
            }
            if(dir == null) {
                // Clone the repo to a temporary directory and return that
                dir = getTempDirectory();
                StringBuilder sb = new StringBuilder();
                sb.append("git clone ");
                sb.append(REPO_CLONE_URL);
                sb.append(" ");
                sb.append(dir);
                try {
                    log.info(
                            "Running {} to clone the concourse repo from Github...",
                            sb.toString());
                    Process p = Runtime.getRuntime().exec(sb.toString());
                    int exitVal = p.waitFor();
                    if(exitVal != 0) {
                        throw new RuntimeException(Processes.getStdErr(p)
                                .toString());
                    }
                }
                catch (Exception e) {
                    throw Throwables.propagate(e);
                }
            }
            LOCAL_REPO_DIR = dir;
        }
        return LOCAL_REPO_DIR;
    }

    /**
     * Return a temporary directory.
     * 
     * @return a temp directory to use in the test
     */
    private static String getTempDirectory() {
        try {
            return Files.createTempDirectory("concourse").toString();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * The cached directory where the repo was cloned to locally for this JVM
     * session.
     */
    private static String LOCAL_REPO_DIR = null;

    // ---logger
    private static final Logger log = LoggerFactory
            .getLogger(ConcourseRepoCloner.class);

    /**
     * The URL from which the repo can be cloned
     */
    private static String REPO_CLONE_URL = "https://github.com/cinchapi/concourse.git";

    /**
     * Valid remote URLs for a local repo's origin or upstream that indicate we
     * can assume that the local repo is cloned or forked from the official
     * repo.
     */
    private static Set<String> VALID_REMOTE_URLS = Sets.newHashSet(
            "https://github.com/cinchapi/concourse.git",
            "git@github.com:cinchapi/concourse.git");

    private ConcourseRepoCloner() {/* noop */}

}
