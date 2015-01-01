/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2015 Jeff Nelson, Cinchapi Software Collective
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.cinchapi.concourse.util;

import java.util.Objects;

import javax.annotation.concurrent.Immutable;

import com.google.common.primitives.Longs;

/**
 * A class that encapsulates the version information for a class contained in a
 * concourse jar.
 * 
 * @author jnelson
 */
@Immutable
public final class Version implements Comparable<Version> {

    /**
     * Return the Version for {@code clazz}.
     * 
     * @param clazz
     * @return the class Version
     */
    public static Version getVersion(Class<?> clazz) {
        return new Version(clazz);
    }

    private final long major;
    private final long minor;
    private final long patch;
    private final String build;

    /**
     * Construct a new instance.
     * 
     * @param clazz
     */
    private Version(Class<?> clazz) {
        String version = (version = clazz.getPackage()
                .getImplementationVersion()) == null ? "UNKNOWN" : version;
        String[] parts = version.split("\\.");
        if(parts.length == 4) {
            this.major = Long.parseLong(parts[0]);
            this.minor = Long.parseLong(parts[1]);
            this.patch = Long.parseLong(parts[2]);
            this.build = parts[3];
        }
        else {
            this.major = 0;
            this.minor = 0;
            this.patch = 0;
            this.build = "0-SNAPSHOT";
        }

    }

    @Override
    public int compareTo(Version o) {
        int comp;
        return (comp = Longs.compare(major, o.major)) == 0 ? (comp = Longs
                .compare(minor, o.minor)) == 0 ? (comp = Longs.compare(patch,
                o.patch)) == 0 ? (build.compareTo(o.build)) : comp : comp
                : comp;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Version) {
            return compareTo((Version) obj) == 0;
        }
        return false;
    }

    /**
     * Returns the build metadata for this Version.
     * 
     * @return the build
     */
    public String getBuild() {
        return build;
    }

    /**
     * Return the major component of this Version.
     * 
     * @return the major
     */
    public long getMajor() {
        return major;
    }

    /**
     * Return the minor component of this Version.
     * 
     * @return the minor
     */
    public long getMinor() {
        return minor;
    }

    /**
     * Return the patch component of this Version.
     * 
     * @return the patch
     */
    public long getPatch() {
        return patch;
    }

    @Override
    public int hashCode() {
        return Objects.hash(major, minor, patch, build);
    }

    @Override
    public String toString() {
        return new StringBuilder().append(major).append(".").append(minor)
                .append(".").append(patch).append(".").append(build).toString();
    }

}
