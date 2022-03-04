/*
 * Copyright (c) 2013-2022 Cinchapi Inc.
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
package com.cinchapi.concourse.server.storage.db.kernel;

import com.cinchapi.concourse.server.io.Composite;
import com.cinchapi.concourse.server.model.Identifier;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.server.storage.db.IndexRevision;

/**
 * An {@link Artifact} containing a {@link IndexRevision}
 *
 * @author Jeff Nelson
 */
public final class IndexArtifact extends Artifact<Text, Value, Identifier> {

    /**
     * Construct a new instance.
     * 
     * @param revision
     * @param composites
     */
    IndexArtifact(IndexRevision revision, Composite[] composites) {
        super(revision, composites);
    }

    @Override
    public IndexRevision revision() {
        return (IndexRevision) super.revision();
    }

}
