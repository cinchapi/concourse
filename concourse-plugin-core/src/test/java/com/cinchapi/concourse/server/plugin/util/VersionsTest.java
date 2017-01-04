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
package com.cinchapi.concourse.server.plugin.util;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.util.Random;
import com.cinchapi.concourse.util.Strings;
import com.github.zafarkhaja.semver.Version;

/**
 * Unit tests for the {@link Versions} util class.
 * 
 * @author Jeff Nelson
 */
public class VersionsTest {

    @Test
    public void testParseCinchapiSemanticVersion() {
        int major = Random.getScaleCount();
        int minor = Random.getScaleCount();
        int patch = Random.getScaleCount();
        int build = Random.getScaleCount();
        String snapshot = Random.getBoolean() ? "-SNAPSHOT" : "";
        String cinchapiVersion = Strings.format("{}.{}.{}.{}{}", major, minor,
                patch, build, snapshot);
        Version version = Versions.parseSemanticVersion(cinchapiVersion);
        Assert.assertEquals(major, version.getMajorVersion());
        Assert.assertEquals(minor, version.getMinorVersion());
        Assert.assertEquals(patch, version.getPatchVersion());
        Assert.assertEquals(String.valueOf(build), version.getBuildMetadata());
    }

    @Test
    public void testParseCinchapiFeatureBranchSemanticVersion() {
        int major = 0;
        int minor = 5;
        int patch = 0;
        int build = 26;
        String snapshot = "-CON-512";
        String cinchapiVersion = Strings.format("{}.{}.{}.{}{}", major, minor,
                patch, build, snapshot);
        Version version = Versions.parseSemanticVersion(cinchapiVersion);
        Assert.assertEquals(major, version.getMajorVersion());
        Assert.assertEquals(minor, version.getMinorVersion());
        Assert.assertEquals(patch, version.getPatchVersion());
        Assert.assertEquals(String.valueOf(build), version.getBuildMetadata());

    }

}
