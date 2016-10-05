package com.cinchapi.concourse.server.io.process;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.lang.SystemUtils;

import com.google.common.base.Throwables;

/**
 * ProcessValidator provides functionality to check if particular process id is
 * running and returns true if it is else it return false. It is designed to be
 * platform-independent. It works for both Linux and Windows platform.
 * 
 * @author raghavbabu
 *
 */
public class ProcessValidator {

    /**
     * Check if the process with the processId is running.
     * 
     * @param processId Id for the input process.
     * @return true if its running, false if not.
     */
    protected static boolean isProcessRunning(String processId) {

        Process process = null;
        try {
            if(SystemUtils.IS_OS_LINUX || SystemUtils.IS_OS_MAC
                    || SystemUtils.IS_OS_MAC_OSX || SystemUtils.IS_OS_UNIX
                    || SystemUtils.IS_OS_SOLARIS || SystemUtils.IS_OS_SUN_OS) {
                process = Runtime.getRuntime().exec("pgrep -f java");
            }
            else if(SystemUtils.IS_OS_WINDOWS) {
                process = Runtime.getRuntime().exec("TASKLIST /fi \"PID eq "
                         +processId+  "\" /fo csv /nh");
            }

        }
        catch (Exception e) {
            Throwables.propagate(e);
        }
        return process != null ? parseOutput(processId, process) : false;
    }

    /**
     * Parse through the process input stream to check if it contains the
     * specific process id.
     * 
     * @param processId
     * @param process
     * @return true if process id present else returns false;
     */
    private static boolean parseOutput(String processId, Process process) {
        BufferedReader in = new BufferedReader(
                new InputStreamReader(process.getInputStream()));
        String line = null;
        try {
            while ((line = in.readLine()) != null) {
                if(line.contains(processId)) {
                    return true;
                }
            }
        }
        catch (IOException e) {
            Throwables.propagate(e);
        }
        return false;
    }
}
