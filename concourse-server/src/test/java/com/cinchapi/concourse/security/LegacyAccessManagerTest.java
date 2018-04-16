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
package com.cinchapi.concourse.security;

import static com.cinchapi.concourse.security.AccessManagerTest.*;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.Maps;

/**
 * Unit tests for {@link com.cinchapi.concourse.security.LegacyAccessManager}.
 *
 * @author knd
 */
public class LegacyAccessManagerTest extends ConcourseBaseTest {

    private String legacy = null;
    private String current = null;
    private LegacyAccessManager legacyManager = null;
    private AccessManager manager = null;

    @Rule
    public TestRule watcher = new TestWatcher() {

        @Override
        protected void finished(Description desc) {
            FileSystem.deleteFile(current);
            FileSystem.deleteFile(legacy);
        }

        @Override
        protected void starting(Description desc) {
            legacy = TestData.DATA_DIR + File.separator + Time.now();
            legacyManager = LegacyAccessManager.create(legacy);
            current = TestData.DATA_DIR + File.separator + Time.now();
            manager = AccessManager.create(current);
        }
    };

    @Test
    public void testUpgradedCredentialsHaveAdmin() {
        createLegacyCredentials();
        legacyManager.diskSync(legacy);
        LegacyAccessManager legacyManager1 = LegacyAccessManager.create(legacy);
        legacyManager1.transferCredentials(manager);
        Assert.assertTrue(manager
                .isExistingUsername(ByteBuffer.wrap("admin".getBytes())));
    }

    @Test
    public void testUpgradedCredentialsHaveAllLegacyCredentials() {
        Map<ByteBuffer, ByteBuffer> legacyCredentials = createLegacyCredentials();
        legacyManager.diskSync(legacy);
        LegacyAccessManager legacyManager1 = LegacyAccessManager.create(legacy);
        legacyManager1.transferCredentials(manager);
        for (Entry<ByteBuffer, ByteBuffer> legacyCreds : legacyCredentials
                .entrySet()) {
            Assert.assertTrue(manager.isExistingUsername(legacyCreds.getKey()));
        }
    }

    @Test
    public void testUpgradedCredentialsHaveSamePasswordsAsBefore() {
        Map<ByteBuffer, ByteBuffer> legacyCredentials = createLegacyCredentials();
        legacyManager.diskSync(legacy);
        LegacyAccessManager legacyManager1 = LegacyAccessManager.create(legacy);
        legacyManager1.transferCredentials(manager);
        for (Entry<ByteBuffer, ByteBuffer> legacyCreds : legacyCredentials
                .entrySet()) {
            Assert.assertTrue(manager.isExistingUsernamePasswordCombo(
                    legacyCreds.getKey(), legacyCreds.getValue()));
        }
    }

    /**
     * Return the mapping from username to password that are created
     * and stored in memory of {@link #legacyManager}.
     * 
     * @param legacyManager
     * @return the mappings from username to password
     */
    private Map<ByteBuffer, ByteBuffer> createLegacyCredentials() {
        Map<ByteBuffer, ByteBuffer> credentials = getLegacyCredentials();
        for (Entry<ByteBuffer, ByteBuffer> creds : credentials.entrySet()) {
            legacyManager.createUser(creds.getKey(), creds.getValue());
        }
        return credentials;
    }

    /**
     * Return the mappings from username to password. This always contains
     * a mapping from binary format of string value <em>admin</em> to
     * binary format of string value <em>admin</em>.
     * 
     * @return the mappings from username to password
     */
    private Map<ByteBuffer, ByteBuffer> getLegacyCredentials() {
        Map<ByteBuffer, ByteBuffer> credentials = Maps.newLinkedHashMap();
        credentials.put(ByteBuffer.wrap("admin".getBytes()),
                ByteBuffer.wrap("admin".getBytes())); // legacy credentials
                                                      // should have "admin"
                                                      // by default
        int testSize = TestData.getScaleCount();
        while (credentials.size() <= testSize) {
            ByteBuffer username = getAcceptableUsername();
            if(!credentials.containsKey(username)) {
                credentials.put(username, getSecurePassword());
            }
        }
        return credentials;
    }

}
