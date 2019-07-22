package com.cinchapi.concourse;

import java.text.MessageFormat;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.cinchapi.concourse.test.ConcourseIntegrationTest;

public class InspectTest extends ConcourseIntegrationTest {

    @Test
    public void testInspect() {
        Map<String, Set<String>> describe = client.inspect();
        
        int enabledPlugins = describe.size();
        
        System.out.println(MessageFormat.format("There are {0} plugins enabled.", enabledPlugins));
        
        describe.forEach((plugin, methods) -> {
            System.out.println(MessageFormat.format("Plugin: {0} | Methods {1}", plugin, methods.toArray().toString()));
        });
    }
    
}
