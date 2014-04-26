package org.cinchapi.concourse;

import org.cinchapi.concourse.thrift.TSecurityException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test security exception which occurs when user session
 * is invalidated from concourse server and the user is kicked
 * out of CaSH session. 
 * 
 * @author knd
 *
 */
public class SecurityExceptionTest extends ConcourseIntegrationTest {
    
    @Test
    public void testTSecurityExceptionIsThrown(){
        try {
            grantAccess("admin", "admin2");
            client.add("name", "brad", 1); // this should throw
                                           // TSecurityException
            Assert.fail("Expecting TSecurityException");
        }
        catch (Exception e) {
            if(e.getCause() != null
                    & e.getCause() instanceof TSecurityException) {
                Assert.assertTrue(true);
            }
            else {
                throw e;
            }
        }
    }
    
}
