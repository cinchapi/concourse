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
package com.cinchapi.concourse.server.upgrade;

import com.cinchapi.concourse.util.Versions;

/**
 * A {@link SmartUpgradeTask} is one that is able to determine its schema
 * version from its name. In order to use functionality correctly, subtask must
 * be a class named with the following format: Upgrade
 * {@code major_minor_patch_task} (i.e {{@code Upgrade0_4_1_1)} for upgrade task
 * 1 for version 0.4.1)
 * 
 * @author Jeff Nelson
 */
public abstract class SmartUpgradeTask extends UpgradeTask {

    @Override
    public int version() {
        String version = getClass().getSimpleName().split("Upgrade")[1]
                .replace("_", ".");
        return (int) Versions.toLongRepresentation(version, 2);
    }

}
