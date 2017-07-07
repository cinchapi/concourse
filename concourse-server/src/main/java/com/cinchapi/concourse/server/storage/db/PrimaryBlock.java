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
package com.cinchapi.concourse.server.storage.db;

import javax.annotation.concurrent.ThreadSafe;

import com.cinchapi.concourse.annotate.DoNotInvoke;
import com.cinchapi.concourse.annotate.PackagePrivate;
import com.cinchapi.concourse.server.model.PrimaryKey;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.server.storage.Action;

/**
 * A Block that stores PrimaryRevisions data to be used in a PrimaryRecord.
 * 
 * @author Jeff Nelson
 */
@ThreadSafe
@PackagePrivate
final class PrimaryBlock extends Block<PrimaryKey, Text, Value> {

    /**
     * DO NOT CALL!!
     * 
     * @param id
     * @param directory
     * @param diskLoad
     */
    @PackagePrivate
    @DoNotInvoke
    PrimaryBlock(String id, String directory, boolean diskLoad) {
        super(id, directory, diskLoad);
    }
    
    @Override
    public final PrimaryRevision insert(PrimaryKey locator, Text key,
            Value value, long version, Action type) {
        return (PrimaryRevision) super
                .insert(locator, key, Value.optimize(value), version, type);
    }

    @Override
    protected PrimaryRevision makeRevision(PrimaryKey locator, Text key,
            Value value, long version, Action type) {
        return Revision.createPrimaryRevision(locator, key, value, version,
                type);
    }

    @Override
    protected Class<PrimaryRevision> xRevisionClass() {
        return PrimaryRevision.class;
    }

}
