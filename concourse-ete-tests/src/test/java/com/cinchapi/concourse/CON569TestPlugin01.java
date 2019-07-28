package com.cinchapi.concourse;

import com.cinchapi.concourse.server.plugin.TestPlugin;

public class CON569TestPlugin01 extends TestPlugin {

    public CON569TestPlugin01(String fromServer, String fromPlugin) {
        super(fromServer, fromPlugin);
    }

    public void say(String message) {}
    
    public void bye() {}

}
