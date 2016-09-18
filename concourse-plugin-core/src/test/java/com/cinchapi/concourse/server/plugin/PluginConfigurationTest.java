package com.cinchapi.concourse.server.plugin;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.List;

public class PluginConfigurationTest {
    @Test
    public void testGetAliases() {
        PluginConfiguration config = new StandardPluginConfiguration(
                Paths.get("/Users/raghavbabu/Desktop/Concourse/upstream/concourse-sep17/concourse/concourse-plugin-core/src/main/resources/plugin.prefs"));
        List<String> actual = Lists.newArrayList();
        actual.add("foo");
        actual.add("bar");
        actual.add("car");
        actual.add("dar");
        actual.add("far");
        List<String> expected = config.getAliases();
        Assert.assertEquals(expected.containsAll(actual),
                actual.containsAll(expected));
    }
}
