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
package com.cinchapi.concourse.server.storage.cache;

import com.cinchapi.concourse.server.io.Byteable;
import com.cinchapi.concourse.server.io.Composite;
import com.cinchapi.concourse.util.ByteBuffers;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

/**
 * A {@link Funnel} for {@link Byteable} objects.
 * 
 * @author Jeff Nelson
 */
public enum ByteableFunnel implements Funnel<Composite> {
    INSTANCE;

    @Override
    public void funnel(Composite from, PrimitiveSink into) {
        into.putBytes(ByteBuffers.toByteArray(from.getBytes()));
    }
}
