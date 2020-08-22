/*
 * Copyright (c) 2013-2020 Cinchapi Inc.
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
package com.cinchapi.concourse.server.io;

import java.nio.ByteBuffer;

import com.cinchapi.lib.offheap.memory.OffHeapMemory;

/**
 *
 *
 * @author jeff
 */
public class OffHeapMemoryByteSink implements ByteSink {

    private final OffHeapMemory memory;

    public OffHeapMemoryByteSink(OffHeapMemory memory) {
        this.memory = memory;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.cinchapi.concourse.server.io.ByteSink#position()
     */
    @Override
    public int position() {
        // TODO Auto-generated method stub
        return 0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.cinchapi.concourse.server.io.ByteSink#put(byte)
     */
    @Override
    public ByteSink put(byte value) {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.cinchapi.concourse.server.io.ByteSink#put(byte[])
     */
    @Override
    public ByteSink put(byte[] src) {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.cinchapi.concourse.server.io.ByteSink#put(java.nio.ByteBuffer)
     */
    @Override
    public ByteSink put(ByteBuffer src) {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.cinchapi.concourse.server.io.ByteSink#putChar(char)
     */
    @Override
    public ByteSink putChar(char value) {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.cinchapi.concourse.server.io.ByteSink#putDouble(double)
     */
    @Override
    public ByteSink putDouble(double value) {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.cinchapi.concourse.server.io.ByteSink#putFloat(float)
     */
    @Override
    public ByteSink putFloat(float value) {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.cinchapi.concourse.server.io.ByteSink#putInt(int)
     */
    @Override
    public ByteSink putInt(int value) {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.cinchapi.concourse.server.io.ByteSink#putLong(long)
     */
    @Override
    public ByteSink putLong(long value) {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.cinchapi.concourse.server.io.ByteSink#putShort(short)
     */
    @Override
    public ByteSink putShort(short value) {
        // TODO Auto-generated method stub
        return null;
    }

}
