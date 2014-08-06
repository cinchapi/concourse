/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2014 Jeff Nelson, Cinchapi Software Collective
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
package org.cinchapi.concourse.server.upgrade;

import org.cinchapi.concourse.util.Versions;

/**
 * A {@link SmartUpgradeTask} is one that is able to determine its schema
 * version from its name. In order to use functionality correctly, subtask must
 * be a class named with the following format: Upgrade
 * {@code major_minor_patch_task} (i.e {{@code Upgrade0_4_1_1)} for upgrade task
 * 1 for version 0.4.1)
 * 
 * @author jnelson
 */
public abstract class SmartUpgradeTask extends UpgradeTask {

    @Override
    public int getSchemaVersion() {
        String version = getClass().getSimpleName().split("Upgrade")[1]
                .replace("_", ".");
        return (int) Versions.toLongRepresentation(version, 2);
    }

}
