package com.cinchapi.concourse;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.cinchapi.concourse.test.ClientServerTest;
import com.cinchapi.concourse.test.PluginTest;
import com.google.common.collect.Maps;

import org.junit.Assert;

public class InspectTest extends ClientServerTest implements PluginTest {

    @Override
    protected String getServerVersion() {
        return ClientServerTest.LATEST_SNAPSHOT_VERSION;
    }
    
    @Test
    public void testInspect() {
        final String TEST_PLUGIN_NAME = "com.cinchapi.concourse.CON569TestPlugin01";
        
        Map<String, Set<String>> descriptions = client.inspect();
        
        Method[] methods = CON569TestPlugin01.class.getMethods();
        
        for (Method method : methods) {
            Parameter[] parameters = method.getParameters();
            StringBuilder builder = new StringBuilder(
                    method.getName());
            builder.append("(");
            for (Parameter parameter : parameters) {
                builder.append(parameter.getType().getName()).append(" ");
            }
            builder.append(")");
            descriptions.get(CON569TestPlugin01.class.getName()).add(builder.toString());
        }
        
        Assert.assertTrue("Descriptions contains no available plugins.", descriptions.isEmpty());
        Assert.assertTrue("Descriptions does not contain the test plugin.", descriptions.containsKey(TEST_PLUGIN_NAME));
        Assert.assertTrue("CON569TestPlugin01 does not contain say(java.lang.String)", descriptions.get(TEST_PLUGIN_NAME).contains("say(java.lang.String )"));
        Assert.assertTrue("CON569TestPlugin01 does not contain bye()", descriptions.get(TEST_PLUGIN_NAME).contains("bye()"));
    }
    


}

