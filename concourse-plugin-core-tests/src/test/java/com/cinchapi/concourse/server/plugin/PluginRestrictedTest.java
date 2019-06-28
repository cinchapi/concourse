package com.cinchapi.concourse.server.plugin;

import com.cinchapi.concourse.test.ClientServerTest;
import com.cinchapi.concourse.test.PluginTest;
import org.junit.Assert;
import org.junit.Test;

public class PluginRestrictedTest extends ClientServerTest implements PluginTest {
    private class ChildPlugin extends TestPlugin {
        public ChildPlugin(String fromServer, String fromPlugin) {
            super(fromServer, fromPlugin);
        }
    }

    @Override
    protected String getServerVersion() {
        return ClientServerTest.LATEST_SNAPSHOT_VERSION;
    }

    @Test
    public void testPluginAnnotationRestriction() {
        try {
            client.invokePlugin(ChildPlugin.class.getName(), "testPluginRestricted");
            Assert.fail();
        } catch (Exception e) { }
    }
}

