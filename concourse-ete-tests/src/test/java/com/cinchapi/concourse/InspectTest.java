package com.cinchapi.concourse;

import org.junit.Test;

import com.cinchapi.concourse.test.ClientServerTest;
import com.cinchapi.concourse.test.PluginTest;

public class InspectTest extends ClientServerTest implements PluginTest {

    @Override
    protected String getServerVersion() {
        return ClientServerTest.LATEST_SNAPSHOT_VERSION;
    }
    
    @Test
    public void testInspect() {
        
        //know expected results
        
//        client.inspect();
        
       //assert if printed results were expected.
    }


    

}

