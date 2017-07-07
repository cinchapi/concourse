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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipInputStream;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.CharStreams;

/**
 * A utility class for handling zip files
 * 
 * @author Jeff Nelson
 */
public final class ZipFiles {

    /**
     * Get the content for the entry at {@code relativeEntryPath} from within
     * the zip file.
     * 
     * @param zipPath the path to the zip file
     * @param relativeEntryPath the path of the entry to extract
     * @return the content of the entry
     */
    public static String getEntryContent(String zipPath,
            String relativeEntryPath) {
        ZipInputStream in = null;
        try {
            in = new ZipInputStream(new FileInputStream(zipPath));
            ZipEntry entry = in.getNextEntry();
            while (entry != null) {
                if(entry.getName().equals(relativeEntryPath)) {
                    return extract(in);
                }
                entry = in.getNextEntry();
            }
            throw new ZipException("Cannot find "+relativeEntryPath);

        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        finally {
            if(in != null) {
                try {
                    in.closeEntry();
                    in.close();
                }
                catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            }
        }
    }

    /**
     * Unzip the contents of the file at {@code zipPath} to the
     * {@code destination} directory.
     * 
     * @param zipPath the absolute path to the zip file
     * @param destination the absolute path to the destination
     */
    public static void unzip(String zipPath, String destination) {
        File dest = new File(destination);
        if(!dest.exists()) {
            dest.mkdirs();
        }
        else {
            Preconditions.checkArgument(dest.isDirectory(),
                    "Unzip destination must be a directory");
        }
        try {
            ZipInputStream in = new ZipInputStream(new FileInputStream(zipPath));
            ZipEntry entry = in.getNextEntry();
            while (entry != null) {
                String target = destination + File.separator + entry.getName();
                if(entry.isDirectory()) {
                    new File(target).mkdirs();
                }
                else {
                    extract(in, target);
                }
                in.closeEntry();
                entry = in.getNextEntry();
            }
            in.close();

        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Extract the {@link ZipInputStream#getNextEntry() current entry} from the
     * {@code in}put stream and place it in the {@code target} path.
     * 
     * @param in the {@link ZipInputStream} that is correctly positioned at the
     *            desired entry
     * @param target the target path for the extracted file
     */
    private static void extract(ZipInputStream in, String target) {
        try {
            BufferedOutputStream baos = new BufferedOutputStream(
                    new FileOutputStream(target));
            byte[] bytesIn = new byte[4096];
            int read = 0;
            while ((read = in.read(bytesIn)) != -1) {
                baos.write(bytesIn, 0, read);
            }
            baos.close();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Extract and return the content of the
     * {@link ZipInputStream#getNextEntry() current entry} from the {@code in}
     * put stream.
     * 
     * @param in the {@link ZipInputStream} that is correctly positioned at the
     *            desired entry
     * @return content of the current entry
     */
    private static String extract(ZipInputStream in) {
        try {
            return CharStreams.toString(new InputStreamReader(in,
                    StandardCharsets.UTF_8));
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
