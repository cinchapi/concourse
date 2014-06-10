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
package org.cinchapi.concourse.server.storage.db;

import javax.annotation.concurrent.ThreadSafe;

import org.cinchapi.concourse.annotate.DoNotInvoke;
import org.cinchapi.concourse.annotate.PackagePrivate;
import org.cinchapi.concourse.server.model.PrimaryKey;
import org.cinchapi.concourse.server.model.Text;
import org.cinchapi.concourse.server.model.Value;
import org.cinchapi.concourse.server.storage.Action;

/**
 * A Block that stores PrimaryRevisions data to be used in a PrimaryRecord.
 * 
 * @author jnelson
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
