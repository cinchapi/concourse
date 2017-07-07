/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse.util;

import java.util.Objects;

import javax.annotation.concurrent.Immutable;

import com.google.common.primitives.Longs;

/**
 * A class that encapsulates the version information for a class contained in a
 * concourse jar.
 * 
 * @author Jeff Nelson
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
