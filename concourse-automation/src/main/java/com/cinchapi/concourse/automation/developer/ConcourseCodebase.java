/*
 * Copyright (c) 2013-2024 Cinchapi Inc.
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
package com.cinchapi.concourse.automation.developer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.common.describe.Empty;
import com.cinchapi.common.process.Processes;
import com.cinchapi.common.process.Processes.ProcessResult;
import com.cinchapi.concourse.util.FileOps;
import com.cinchapi.concourse.util.Platform;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

/**
 * Provides programmatic interaction with a local copy of the Concourse source
 * code.
 * <p>
 * This handler can detect if it is running from code that is part of the
 * Concourse codebase (e.g., an internal project) and behave appropriately.
 * </p>
 *
 * @author Jeff Nelson
 */
public final class ConcourseCodebase {

    /**
     * If necessary, clone the codebase to a local directory and
     * return a handler than can be used for programmatic interaction.
     * <p>
     * <strong>NOTE:</strong> Multiple attempts to {@link #cloneFromGithub()}
     * will return the same {@link ConcourseCodebase codebase object} for the
     * lifetime of the JVM process.
     * </p>
     * 
     * @return the handler to interact with the codebase
     */
    public static ConcourseCodebase get() {
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
                            "--get", "remote.origin.url")
                                    .directory(new File(dir)).start();
                    Process upstreamProc = new ProcessBuilder("git", "config",
                            "--get", "remote.upstream.url")
                                    .directory(new File(dir)).start();
                    List<String> originLines = Processes.getStdOut(originProc);
                    List<String> upstreamLines = Processes
                            .getStdOut(upstreamProc);
                    String originOut = !originLines.isEmpty()
                            ? originLines.get(0)
                            : "";
                    String upstreamOut = !upstreamLines.isEmpty()
                            ? upstreamLines.get(0)
                            : "";
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
            Path cache = Paths.get(REPO_CACHE_FILE);
            if(dir == null) {
                // If last cloned dir still exists use it, but perform git pull
                // to fetch latest changes
                if(cache.toFile().exists()) {
                    List<String> list = FileOps.readLines(REPO_CACHE_FILE);
                    if(list.size() > 0) {
                        dir = list.iterator().next();
                        if(!Paths.get(dir, ".git", "index").toFile().exists()) {
                            dir = null;
                            cache.toFile().delete();
                        }
                    }
                }
                if(dir != null) {
                    try {
                        LOGGER.info(
                                "Running 'git pull' to fetch latest changes from Github...");
                        ProcessBuilder pb = Processes.getBuilder("git", "pull");
                        pb.directory(new File(dir));
                        Process p = pb.start();
                        int exitVal = p.waitFor();
                        if(exitVal != 0) {
                            throw new RuntimeException(
                                    Processes.getStdErr(p).toString());
                        }
                    }
                    catch (Exception e) {
                        throw CheckedExceptions.wrapAsRuntimeException(e);
                    }
                }
                else {
                    // If we're not currently in a github clone/fork, then go
                    // ahead and clone to some temp directory
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
                            throw new RuntimeException(
                                    Processes.getStdErr(p).toString());
                        }
                        // store path of the clone
                        cache.toFile().getParentFile().mkdirs();
                        cache.toFile().createNewFile();
                        FileOps.write(dir, cache.toString());
                    }
                    catch (Exception e) {
                        throw CheckedExceptions.wrapAsRuntimeException(e);
                    }
                }
            }
            INSTANCE = new ConcourseCodebase(Paths.get(dir));
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
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
    }

    /**
     * File in user.home directory that will hold path to last clone
     */
    @VisibleForTesting
    protected static final String REPO_CACHE_FILE = Paths
            .get(System.getProperty("user.home"), ".cinchapi", "concourse",
                    "codebase", "cache.location")
            .toString();

    /**
     * Singleton instance.
     */
    @VisibleForTesting
    protected static ConcourseCodebase INSTANCE = null;

    /**
     * The name of the file that contains a cache of the state of the codebase.
     */
    private static String CODE_STATE_CACHE_FILENAME = ".codestate";

    /**
     * The URL from which the repo can be cloned
     */
    private static String GITHUB_CLONE_URL = "https://github.com/cinchapi/concourse.git";

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
    private final Path path;

    /**
     * Construct a new instance.
     * 
     * @param path - the path to the codebase
     */
    private ConcourseCodebase(Path path) {
        this.path = path;
    }

    /**
     * If necessary, build a concourse-server installer (e.g.,
     * concourse-server.bin) from this {@link ConcourseCodebase codebase) and
     * return the {@link Path} to the file.
     * 
     * @return the {@link Path} to the installer filer
     */
    public Path installer() {
        try {
            if(!hasInstaller() || hasCodeChanged()) {
                LOGGER.info("A code change was detected, so a NEW installer "
                        + "is being generated.");
                Process p = new ProcessBuilder("./gradlew", "clean",
                        "installer").directory(path.toFile()).start();
                Processes.waitForSuccessfulCompletion(p);
                LOGGER.info("Finished generating installer.");
            }
            return getInstallerPath();
        }
        catch (Exception e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
    }

    /**
     * Return the {@link Path} where this {@link ConcourseCodebase codebase}
     * resides on disk.
     * 
     * @return the {@link Path} to the codebase
     */
    public Path path() {
        return path;
    }

    /**
     * Return the path to the installer file for this codebase or an empty
     * string if no installer file exists.
     * 
     * @return the path to the installer file
     */
    @Nullable
    private Path getInstallerPath() {
        try {
            String cmd = new StringBuilder().append("ls -a ").append(path)
                    .append("/concourse-server/build/distributions | grep bin")
                    .toString();
            Process p = Processes.getBuilderWithPipeSupport(cmd).start();
            try {
                ProcessResult result = Processes.waitForSuccessfulCompletion(p);
                String installer = result.out().get(0);
                if(!installer.isEmpty()) {
                    installer = path + "/concourse-server/build/distributions/"
                            + installer;
                }
                return Paths.get(installer);
            }
            catch (Exception e) {
                return null;
            }
        }
        catch (Exception e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
    }

    /**
     * Check to see if there has been a code change since the last check.
     * 
     * @return {@code true} if it appears that the code has changed
     */
    private boolean hasCodeChanged() {
        Path cache = path().resolve(CODE_STATE_CACHE_FILENAME).toAbsolutePath();
        String cmd = "(git status; git diff; git log -n 1) | "
                + (Platform.isMacOsX() ? "md5" : "md5sum");
        try {
            Process p = Processes.getBuilderWithPipeSupport(cmd)
                    .directory(path().toFile()).start();
            ProcessResult result = Processes.waitForSuccessfulCompletion(p);
            String state = result.out().get(0);
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
            throw CheckedExceptions.wrapAsRuntimeException(e);
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
        return !Empty.ness().describes(getInstallerPath());
    }

}
