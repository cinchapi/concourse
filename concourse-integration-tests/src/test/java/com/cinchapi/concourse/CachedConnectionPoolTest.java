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
package com.cinchapi.concourse;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.CachedConnectionPool;
import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.ConnectionPool;
import com.google.common.collect.Lists;

/**
 * Unit tests for {@link CachedConnectionPool}.
 * 
 * @author Jeff Nelson
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
