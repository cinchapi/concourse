/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2014 Jeff Nelson, Cinchapi Software Collective
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
package org.cinchapi.concourse.security;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Map.Entry;
import org.cinchapi.concourse.ConcourseBaseTest;
import org.cinchapi.concourse.server.io.FileSystem;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.TestData;
import static org.cinchapi.concourse.security.AccessManagerTest.*;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.google.common.collect.Maps;

/**
 * Unit tests for {@link LegacyAccessManager}.
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
        LegacyAccessManager legacyManager1 = LegacyAccessManager
                .create(legacy);                                   
        legacyManager1.transferCredentials(manager);
        Assert.assertTrue(manager.isValidUsername(
                ByteBuffer.wrap("admin".getBytes())));
    }
    
    @Test
    public void testUpgradedCredentialsHaveAllLegacyCredentials() {
        Map<ByteBuffer, ByteBuffer> legacyCredentials = 
                createLegacyCredentials();
        legacyManager.diskSync(legacy);
        LegacyAccessManager legacyManager1 = LegacyAccessManager
                .create(legacy);                                   
        legacyManager1.transferCredentials(manager);
        for (Entry<ByteBuffer, ByteBuffer> legacyCreds : 
            legacyCredentials.entrySet()) {
            Assert.assertTrue(manager.isValidUsername(
                    legacyCreds.getKey()));
        }
    }
    
    @Test
    public void testUpgradedCredentialsHaveSamePasswordsAsBefore() {
        Map<ByteBuffer, ByteBuffer> legacyCredentials = 
                createLegacyCredentials();
        legacyManager.diskSync(legacy);
        LegacyAccessManager legacyManager1 = LegacyAccessManager
                .create(legacy);                                   
        legacyManager1.transferCredentials(manager);
        for (Entry<ByteBuffer, ByteBuffer> legacyCreds : 
            legacyCredentials.entrySet()) {
            Assert.assertTrue(manager.isValidUserNamePasswordCombo(
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
                ByteBuffer.wrap("admin".getBytes()));        // legacy credentials
                                                             // should have "admin"
                                                             // by default
        int testSize = TestData.getScaleCount();
        while (credentials.size() <= testSize) {
            ByteBuffer username = getAcceptableUsername();
            if (!credentials.containsKey(username)) {
                credentials.put(username, getSecurePassword());
            }
        }
        return credentials;
    }
    
}
