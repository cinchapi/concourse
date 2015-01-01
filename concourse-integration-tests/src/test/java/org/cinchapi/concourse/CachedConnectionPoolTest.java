/*
 * The MIT License (MIT)
 * 
 * 2013-2015 Jeff Nelson, Cinchapi Software Collective
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
package org.cinchapi.concourse;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Unit tests for {@link CachedConnectionPool}.
 * 
 * @author jnelson
 */
public class CachedConnectionPoolTest extends ConnectionPoolTest {

    @Test
    public void testNoLimitToNumberOfConnectionsInPool(){
        List<Concourse> toReturn = Lists.newArrayList();
        while(toReturn.size() < ConnectionPool.DEFAULT_POOL_SIZE + 1){
            toReturn.add(connections.request());
        }
        //Basically, if this test reaches this point, it is successful
        Assert.assertTrue(toReturn.size() > ConnectionPool.DEFAULT_POOL_SIZE);
        for (Concourse concourse : toReturn) {
            // must return all the connections so the pool can shutdown after
            // the test
            connections.release(concourse);
        }
    }
    
    @Override
    protected ConnectionPool getConnectionPool() {
        return ConnectionPool.newCachedConnectionPool(SERVER_HOST, SERVER_PORT,
                USERNAME, PASSWORD);
    }


    @Override
    protected ConnectionPool getConnectionPool(String env) {
        return ConnectionPool.newCachedConnectionPool(SERVER_HOST, SERVER_PORT,
                USERNAME, PASSWORD, env);
    }

}
