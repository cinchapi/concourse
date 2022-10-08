/*
 * Copyright (c) 2013-2022 Cinchapi Inc.
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

import java.io.FileOutputStream;
import java.net.URI;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.Iterator;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.concourse.util.FileOps;

/**
 * Provides programmatic ability to retrieve local copies of Concourse artifacts
 * for any
 * version.
 *
 * @author Jeff Nelson
 */
public final class ConcourseArtifacts {

    /**
     * Get the {@link Path} to the Concourse Server installer for the specified
     * {@code version} into the user's home directory.
     * 
     * @param version
     * @return the absolute {@link Path} to the installer file
     */
    public static Path installer(String version) {
        return installer(version, Paths.get(FileOps.getUserHome()));
    }

    /**
     * Get the {@link Path} to the Concourse Server installer for the specified
     * {@code version} into the specified {@code location}.
     * 
     * @param version
     * @param location
     * @return the absolute {@link Path} to the installer file
     */
    public static Path installer(String version, Path location) {
        Path file = location.resolve("concourse-server-" + version + ".bin");
        if(!Files.exists(file)) {
            log.info(MessageFormat.format(
                    "Did not find an installer for "
                            + "ConcourseServer v{0} in {1}",
                    version, location));
            URL url;
            try (FileOutputStream stream = new FileOutputStream(
                    file.toFile())) {
                url = new URI(getDownloadUrl(version)).toURL();
                ReadableByteChannel channel = Channels
                        .newChannel(url.openStream());
                stream.getChannel().transferFrom(channel, 0, Long.MAX_VALUE);
                log.info(MessageFormat.format("Downloaded the installer for "
                        + "Concourse Server v{0} from {1}. The installer is "
                        + "stored in {2}", version, url.toString(), location));
            }
            catch (Exception e) {
                throw CheckedExceptions.wrapAsRuntimeException(e);
            }
        }
        return file;
    }

    /**
     * Return the download URL for the specified {@code version} of Concourse
     * Server.
     * 
     * @param version
     * @return the download URL
     */
    private static String getDownloadUrl(String version) {
        String page = RELEASE_PAGE_URL_BASE + version;
        try {
            Document doc = Jsoup.parse(new URI(page).toURL(), 10000);
            Elements links = doc.select("a[href]");
            Iterator<Element> it = links.iterator();
            while (it.hasNext()) {
                Element element = it.next();
                String url = element.attr("href");
                System.out.println(url);
                if(url.endsWith(".bin")) {
                    return DOWNLOAD_URL_BASE + url;
                }
            }
            throw new Exception(
                    "Could not determine download URL for version " + version);
        }
        catch (Exception e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
    }

    /**
     * The base Github URL for the release page.
     */
    private static final String RELEASE_PAGE_URL_BASE = "https://github.com/cinchapi/concourse/releases/expanded_assets/v";

    /**
     * The base Github URL for the download page.
     */
    private static final String DOWNLOAD_URL_BASE = "https://github.com";

    // ---logger
    private static final Logger log = LoggerFactory
            .getLogger(ConcourseArtifacts.class);

    private ConcourseArtifacts() {}

}
