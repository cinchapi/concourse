/*
 * Copyright (c) 2013-2021 Cinchapi Inc.
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

import javax.annotation.concurrent.Immutable;

import com.cinchapi.concourse.server.io.Byteable;
import com.cinchapi.concourse.server.io.Composite;
import com.cinchapi.concourse.server.storage.db.Revision;

/**
 * An {@link Artifact} groups a {@link Revision} and the various
 * {@link Composite Composites} that can be used to locate caches where there
 * {@link Revision} should be added.
 *
 * @author Jeff Nelson
 */
@Immutable
public abstract class Artifact<L extends Byteable & Comparable<L>, K extends Byteable & Comparable<K>, V extends Byteable & Comparable<V>> {

    /**
     * The {@link Composite Composites}.
     */
    private final Composite[] composites;

    /**
     * The {@link Revision}.
     */
    private final Revision<L, K, V> revision;

    /**
     * Construct a new instance.
     * 
     * @param revision
     * @param composites
     */
    protected Artifact(Revision<L, K, V> revision, Composite[] composites) {
        this.revision = revision;
        this.composites = composites;
    }

    /**
     * Return the {@link Composite Composites}
     * 
     * @return the {@link Composite Composites}
     */
    public Composite[] composites() {
        return composites;
    }

    /**
     * Return the {@link Composite} that includes the {@code L locator} of the
     * {@link #revision() revision}.
     * 
     * @return the {@link Composite}
     */
    public Composite getLocatorComposite() {
        // Caution: This leverages implementation details of Chunk#insertUnsafe
        return composites[0];
    }

    /**
     * Return the {@link Composite} that includes the {@code L locator} and
     * {@code K key} of the {@link #revision() revision}.
     * 
     * @return the {@link Composite}
     */
    public Composite getLocatorKeyComposite() {
        // Caution: This leverages implementation details of Chunk#insertUnsafe
        return composites[1];
    }

    /**
     * Return the {@link Composite} that includes the {@code L locator},
     * {@code K key} and {@code V value} of the {@link #revision() revision}.
     * 
     * @return the {@link Composite}
     */
    public Composite getLocatorKeyValueComposite() {
        // Caution: This leverages implementation details of Chunk#insertUnsafe
        return composites[2];
    }

    /**
     * Return the {@link Revision}
     * 
     * @return the {@link Revision}
     */
    public Revision<L, K, V> revision() {
        return revision;
    }
}
