/*
 * Copyright (c) 2013-2016 Cinchapi Inc.
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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.collect.Sets;

/**
 * An object that can be used to programmatically interact with a local instance
 * of the Concourse codebase.
 * 
 * @author Jeff Nelson
 */
public class ConcourseCodebase {

    /**
     * If necessary, clone the codebase from Github to a local directory and
     * return a handler than can be used for programmatic interaction. Multiple
     * attempts to clone the codebase from Github will return the same
     * {@link ConcourseCodebase codebase object} for the lifetime of the JVM
     * process.
     * 
     * @return the handler to interact with the codebase
     */
    public static ConcourseCodebase cloneFromGithub() {
        if(INSTANCE == null) {
            // First we must check to see if the current JVM process is running
            // within a directory that is a clone/fork of the github repo. We
            // must keep checking the parent directories until we reach the root
            // of the repo.
            String dir = System.getProperty("user.dir");
            String odir = null;
            boolean checkParent = true;
            while (checkParent) {
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
                // If we're not currently in a github clone/fork, then go ahead
                // and clone to some temp directory
                dir = getTempDirectory();
                StringBuilder sb = new StringBuilder();
                sb.append("git clone ");
                sb.append(GITHUB_CLONE_URL);
                sb.append(" ");
                sb.append(dir);
                try {
                    LOGGER.info(
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
            INSTANCE = new ConcourseCodebase(dir);
        }
        return INSTANCE;
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
     * The name of the file that contains a cache of the state of the codebase.
     */
    private static String CODE_STATE_CACHE_FILENAME = ".codestate";

    /**
     * The URL from which the repo can be cloned
     */
    private static String GITHUB_CLONE_URL = "https://github.com/cinchapi/concourse.git";

    private static ConcourseCodebase INSTANCE = null;

    /**
     * The Logger instance.
     */
    private static final Logger LOGGER = LoggerFactory
            .getLogger(ConcourseCodebase.class);

    /**
     * Valid remote URLs for a local repo's origin or upstream that indicate we
     * can assume that the local repo is cloned or forked from the official
     * repo.
     */
    private static Set<String> VALID_REMOTE_URLS = Sets.newHashSet(
            "https://github.com/cinchapi/concourse.git",
            "git@github.com:cinchapi/concourse.git");

    /**
     * The path to the codebase on the local machine.
     */
    private final String path;

    /**
     * Construct a new instance.
     * 
     * @param path - the path to the codebase
     */
    private ConcourseCodebase(String path) {
        this.path = path;
    }

    /**
     * Create a concourse-server.bin installer from this
     * {@link ConcourseCodebase codebase} and return the path to the installer
     * file.
     * 
     * @return the path to the installer file
     */
    public String buildInstaller() {
        try {
            if(!hasInstaller() || hasCodeChanged()) {
                Process p = Processes
                        .getBuilder("bash", "gradlew", "clean", "installer")
                        .directory(new File(path)).start();
                Processes.waitForSuccessfulCompletion(p);
            }
            return getInstallerPath();
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Return the path to this {@link ConcourseCodebase codebase}.
     * 
     * @return the path to the codebase
     */
    public String getPath() {
        return path;
    }

    /**
     * Return the path to the installer file for this codebase or an empty
     * string if no installer file exists.
     * 
     * @return the path to the installer file
     */
    private String getInstallerPath() {
        try {
            String cmd = new StringBuilder().append("ls -a ").append(path)
                    .append("/concourse-server/build/distributions | grep bin")
                    .toString();
            Process p = Processes.getBuilderWithPipeSupport(cmd).start();
            try {
                Processes.waitForSuccessfulCompletion(p);
                String installer = Processes.getStdOut(p).get(0);
                if(!installer.isEmpty()) {
                    installer = path + "/concourse-server/build/distributions/"
                            + installer;
                }
                return installer;
            }
            catch (Exception e) {
                return "";
            }
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Check to see if there has been a code change since the last check.
     * 
     * @return {@code true} if it appears that the code has changed
     */
    private boolean hasCodeChanged() {
        Path cache = Paths.get(getPath(), CODE_STATE_CACHE_FILENAME)
                .toAbsolutePath();
        String cmd = Platform.isMacOsX() ? "(git status; git diff) | md5"
                : "(git status; git diff) | md5sum";
        try {
            Process p = Processes.getBuilderWithPipeSupport(cmd)
                    .directory(new File(getPath())).start();
            Processes.waitForSuccessfulCompletion(p);
            String state = Processes.getStdOut(p).get(0);
            FileOps.touch(cache.toString());
            String cached = FileOps.read(cache.toString());
            boolean changed = false;
            if(!state.equals(cached)) {
                FileOps.write(state, cache.toString());
                changed = true;
            }
            return changed;
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Return {@code true} if the codebase has an installer file, {@code false}
     * otherwise.
     * 
     * @return a boolean that indicates whether the codebase has an installer
     *         file or not
     */
    private boolean hasInstaller() {
        return !getInstallerPath().isEmpty();
    }

}
