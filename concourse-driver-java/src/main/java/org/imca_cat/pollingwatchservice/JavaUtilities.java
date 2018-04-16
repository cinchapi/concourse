/*
 * Copyright (c) 2013-2018 Cinchapi Inc.
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

import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;

/**
 * Static utility methods to augment the core Java library.
 */
class JavaUtilities {
    private JavaUtilities() {
        throw new AssertionError();
    }

    /*
     * We can find no guarantee that the BasicFileAttributes implementors
     * override Object#equals, so define our own to be sure.
     */
    public static boolean equals(BasicFileAttributes a, BasicFileAttributes b) {
        if(a == b)
            return true;
        if(a == null || b == null)
            return false;
        if(!a.creationTime().equals(b.creationTime()))
            return false;
        if(!Objects.equals(a.fileKey(), b.fileKey()))
            return false;
        if(a.isDirectory() != b.isDirectory())
            return false;
        if(a.isOther() != b.isOther())
            return false;
        if(a.isRegularFile() != b.isRegularFile())
            return false;
        if(a.isSymbolicLink() != b.isSymbolicLink())
            return false;
        if(!a.lastAccessTime().equals(b.lastAccessTime()))
            return false;
        if(!a.lastModifiedTime().equals(b.lastModifiedTime()))
            return false;
        if(a.size() != b.size())
            return false;
        return true;
    }

    /*
     * BasicFileAttributes instances are considered equal if all of their
     * members
     * are equal except for lastAccessTime, which is ignored since polling can
     * change it.
     */
    public static boolean equalsIgnoreLastAccessTime(BasicFileAttributes a,
            BasicFileAttributes b) {
        if(a == b)
            return true;
        if(a == null || b == null)
            return false;
        if(!a.creationTime().equals(b.creationTime()))
            return false;
        if(!Objects.equals(a.fileKey(), b.fileKey()))
            return false;
        if(a.isDirectory() != b.isDirectory())
            return false;
        if(a.isOther() != b.isOther())
            return false;
        if(a.isRegularFile() != b.isRegularFile())
            return false;
        if(a.isSymbolicLink() != b.isSymbolicLink())
            return false;
        if(!a.lastModifiedTime().equals(b.lastModifiedTime()))
            return false;
        if(a.size() != b.size())
            return false;
        return true;
    }

    /*
     * Some BasicFileAttributes implementors don't override Object#toString, so
     * define our own.
     */
    public static String toString(BasicFileAttributes a) {
        StringBuilder result = new StringBuilder();
        result.append("(creationTime=");
        result.append(a.creationTime());
        result.append(",fileKey=");
        result.append(a.fileKey());
        result.append(",isDirectory=");
        result.append(a.isDirectory());
        result.append(",isOther=");
        result.append(a.isOther());
        result.append(",isRegularFile=");
        result.append(a.isRegularFile());
        result.append(",isSymbolicLink=");
        result.append(a.isSymbolicLink());
        result.append(",lastAccessTime=");
        result.append(a.lastAccessTime());
        result.append(",lastModifiedTime=");
        result.append(a.lastModifiedTime());
        result.append(",size=");
        result.append(a.size());
        result.append(")");
        return result.toString();
    }
}
