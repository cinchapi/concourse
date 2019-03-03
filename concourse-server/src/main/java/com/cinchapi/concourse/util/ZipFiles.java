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
package com.cinchapi.concourse.util;

import java.io.File;
import java.nio.ByteBuffer;

import org.zeroturnaround.zip.ZipUtil;

import com.cinchapi.common.base.Verify;
import com.cinchapi.common.io.ByteBuffers;

/**
 * A utility class for handling zip files
 * 
 * @author Jeff Nelson
 */
public final class ZipFiles {

    static {
        Logging.disable(ZipUtil.class);
    }

    /**
     * Get the content for the entry at {@code relativeEntryPath} from within
     * the zip file.
     * 
     * @param zipPath the path to the zip file
     * @param relativeEntryPath the path of the entry to extract
     * @return the content of the entry as a {@link ByteBuffer}
     */
    public static ByteBuffer getEntryContent(String zipFile,
            String relativeEntryPath) {
        return ByteBuffer.wrap(
                ZipUtil.unpackEntry(new File(zipFile), relativeEntryPath));
    }

    /**
     * Get the content for the entry at {@code relativeEntryPath} from within
     * the zip file as a UTF-8 string.
     * 
     * @param zipPath the path to the zip file
     * @param relativeEntryPath the path of the entry to extract
     * @return the content of the entry as a UTF-8 string
     */
    public static String getEntryContentUtf8(String zipFile,
            String relativeEntryPath) {
        return ByteBuffers
                .getUtf8String(getEntryContent(zipFile, relativeEntryPath));
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
        Verify.thatArgument(dest.isDirectory(),
                "Unzip destination must be a directory");
        try {
            ZipUtil.unpack(new File(zipPath), dest);
        }
        catch (org.zeroturnaround.zip.ZipException e) {
            IllegalArgumentException ex = new IllegalArgumentException(
                    e.getMessage());
            ex.setStackTrace(e.getStackTrace());
            throw ex;
        }
    }

}
