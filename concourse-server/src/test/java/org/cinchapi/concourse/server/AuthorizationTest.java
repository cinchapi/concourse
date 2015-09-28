/*
 * Copyright (c) 2013-2015 Cinchapi Inc.
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
package org.cinchapi.concourse.server;

import org.apache.thrift.TException;
import org.cinchapi.concourse.thrift.AccessToken;
import org.cinchapi.concourse.thrift.TSecurityException;
import org.cinchapi.concourse.util.ByteBuffers;
import org.cinchapi.concourse.thrift.TransactionToken;
import org.junit.Assert;
import org.junit.Test;

/**
 * A unit test that validates the authorization functionality in
 * {@link ConcourseServer}.
 * 
 * @author Jeff Nelson
 */
public class AuthorizationTest extends ConcourseServerBaseTest {

//    @Test
//    public void testCheckAccessIsCalled() throws TException {
//        String env = "";
//        TransactionToken transaction = null;
//        AccessToken creds = server.login(ByteBuffers.fromString("admin"),
//                ByteBuffers.fromString("admin"), env);
//        server.getServerEnvironment(creds, transaction, env);
//        server.logout(creds, env);
//        try {
//            server.getServerEnvironment(creds, transaction, env);
//            Assert.fail("Expecting a TSecurityException");
//        }
//        catch (TSecurityException e) {
//            Assert.assertTrue(true);
//        }
//    }
    
    @Test
    public void testFoo() throws TException{
        String env = "";
        TransactionToken transaction = null;
        AccessToken creds = server.login(ByteBuffers.fromString("admin"),
              ByteBuffers.fromString("admin"), env);
        server.getKeyRecordTimestr("foo", 1, "hdgas", creds, transaction, env);
    }

}
