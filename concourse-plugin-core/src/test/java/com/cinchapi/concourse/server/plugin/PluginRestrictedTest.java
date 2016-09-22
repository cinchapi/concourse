package com.cinchapi.concourse.server.plugin;

import java.nio.ByteBuffer;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class PluginRestrictedTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testIfEnableUserRestrictedToPlugin()
            throws NoSuchMethodException, SecurityException {
        ConcourseRuntime runtime = ConcourseRuntime.getRuntime();
        thrown.expect(NoSuchMethodException.class);
        runtime.getClass().getDeclaredMethod("enableUser", Byte[].class);
    }

    @Test
    public void testIfGetDumpListRestrictedToPlugin()
            throws NoSuchMethodException, SecurityException {
        ConcourseRuntime runtime = ConcourseRuntime.getRuntime();
        thrown.expect(NoSuchMethodException.class);
        runtime.getClass().getDeclaredMethod("getDumpList", String.class);
        runtime.getClass().getDeclaredMethod("getDumpList");
    }

    @Test
    public void testIfGrantRestrictedToPlugin()
            throws NoSuchMethodException, SecurityException {
        ConcourseRuntime runtime = ConcourseRuntime.getRuntime();
        thrown.expect(NoSuchMethodException.class);
        runtime.getClass().getDeclaredMethod("grant", Byte[].class,
                Byte[].class);
    }

    @Test
    public void testIfLoginRestrictedToPlugin()
            throws NoSuchMethodException, SecurityException {
        ConcourseRuntime runtime = ConcourseRuntime.getRuntime();
        thrown.expect(NoSuchMethodException.class);
        runtime.getClass().getDeclaredMethod("login", Byte[].class,
                Byte[].class);
    }

    @Test
    public void testIfLoginMethodRestrictedToPlugin()
            throws NoSuchMethodException, SecurityException {
        ConcourseRuntime runtime = ConcourseRuntime.getRuntime();
        thrown.expect(NoSuchMethodException.class);
        runtime.getClass().getDeclaredMethod("login", ByteBuffer.class,
                ByteBuffer.class, String.class);
    }

    @Test
    public void testIfstartRestrictedToPlugin()
            throws NoSuchMethodException, SecurityException {
        ConcourseRuntime runtime = ConcourseRuntime.getRuntime();
        thrown.expect(NoSuchMethodException.class);
        runtime.getClass().getDeclaredMethod("start");
    }

    @Test
    public void testIfstopRestrictedToPlugin()
            throws NoSuchMethodException, SecurityException {
        ConcourseRuntime runtime = ConcourseRuntime.getRuntime();
        thrown.expect(NoSuchMethodException.class);
        runtime.getClass().getDeclaredMethod("stop");
    }

    @Test
    public void testIfUninstallPluginBundleRestrictedToPlugin()
            throws NoSuchMethodException, SecurityException {
        ConcourseRuntime runtime = ConcourseRuntime.getRuntime();
        thrown.expect(NoSuchMethodException.class);
        runtime.getClass().getDeclaredMethod("uninstallPluginBundle",
                ByteBuffer.class, ByteBuffer.class, String.class);
    }

    @Test
    public void testIfRevokeRestrictedToPlugin()
            throws NoSuchMethodException, SecurityException {
        ConcourseRuntime runtime = ConcourseRuntime.getRuntime();
        thrown.expect(NoSuchMethodException.class);
        runtime.getClass().getDeclaredMethod("revoke", ByteBuffer.class,
                ByteBuffer.class, String.class);
    }

    @Test
    public void testIfHasUserRestrictedToPlugin()
            throws NoSuchMethodException, SecurityException {
        ConcourseRuntime runtime = ConcourseRuntime.getRuntime();
        thrown.expect(NoSuchMethodException.class);
        runtime.getClass().getDeclaredMethod("hasUser", Byte[].class);
    }

    @Test
    public void testIfDumpRestrictedToPlugin()
            throws NoSuchMethodException, SecurityException {
        ConcourseRuntime runtime = ConcourseRuntime.getRuntime();
        thrown.expect(NoSuchMethodException.class);
        runtime.getClass().getDeclaredMethod("dump", String.class,
                String.class);
    }

    @Test
    public void testIfDumpMethodRestrictedToPlugin()
            throws NoSuchMethodException, SecurityException {
        ConcourseRuntime runtime = ConcourseRuntime.getRuntime();
        thrown.expect(NoSuchMethodException.class);
        runtime.getClass().getDeclaredMethod("dump", String.class);
    }   
}
