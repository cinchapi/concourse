/*
 * Copyright (c) 2014 J. Lewis Muir <jlmuir@imca-cat.org>
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
    if (a == b) return true;
    if (a == null || b == null) return false;
    if (!a.creationTime().equals(b.creationTime())) return false;
    if (!Objects.equals(a.fileKey(), b.fileKey())) return false;
    if (a.isDirectory() != b.isDirectory()) return false;
    if (a.isOther() != b.isOther()) return false;
    if (a.isRegularFile() != b.isRegularFile()) return false;
    if (a.isSymbolicLink() != b.isSymbolicLink()) return false;
    if (!a.lastAccessTime().equals(b.lastAccessTime())) return false;
    if (!a.lastModifiedTime().equals(b.lastModifiedTime())) return false;
    if (a.size() != b.size()) return false;
    return true;
  }

  /*
   * BasicFileAttributes instances are considered equal if all of their members
   * are equal except for lastAccessTime, which is ignored since polling can
   * change it.
   */
  public static boolean equalsIgnoreLastAccessTime(BasicFileAttributes a, BasicFileAttributes b) {
    if (a == b) return true;
    if (a == null || b == null) return false;
    if (!a.creationTime().equals(b.creationTime())) return false;
    if (!Objects.equals(a.fileKey(), b.fileKey())) return false;
    if (a.isDirectory() != b.isDirectory()) return false;
    if (a.isOther() != b.isOther()) return false;
    if (a.isRegularFile() != b.isRegularFile()) return false;
    if (a.isSymbolicLink() != b.isSymbolicLink()) return false;
    if (!a.lastModifiedTime().equals(b.lastModifiedTime())) return false;
    if (a.size() != b.size()) return false;
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
