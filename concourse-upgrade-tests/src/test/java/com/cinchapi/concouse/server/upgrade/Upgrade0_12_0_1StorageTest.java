/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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
package com.cinchapi.concouse.server.upgrade;

import com.cinchapi.common.base.Array;
import com.cinchapi.concourse.test.StorageUpgradeTest;

/**
 * Test that data integrity is still valid after 0.12 upgrade.
 *
 * @author Jeff Nelson
 */
public class Upgrade0_12_0_1StorageTest extends StorageUpgradeTest {

    @Override
    protected String[] envs() {
        return Array.containing("default");
    }

    @Override
    protected String getInitialServerVersion() {
        return "0.11.5";
    }

}
