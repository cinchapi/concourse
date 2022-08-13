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
import com.cinchapi.concourse.server.model.Position;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.storage.db.CorpusRevision;

/**
 * An {@link Artifact} containing a {@link CorpusRevision}
 *
 * @author Jeff Nelson
 */
public final class CorpusArtifact extends Artifact<Text, Text, Position> {

    /**
     * Construct a new instance.
     * 
     * @param revision
     * @param composites
     */
    CorpusArtifact(CorpusRevision revision, Composite[] composites) {
        super(revision, composites);
    }

    @Override
    public CorpusRevision revision() {
        return (CorpusRevision) super.revision();
    }

}
