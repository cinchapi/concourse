/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013 Jeff Nelson, Cinchapi Software Collective
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.cinchapi.concourse.server.jmx;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import javax.management.MXBean;

/**
 * An MXBean that defines managed operations for {@link ConcourseServer}.
 * 
 * @author jnelson
 */
@MXBean
public interface ConcourseServerMXBean {

    // Operations
    public static String DUMP_OPRERATION = "dump";

    /**
     * The JMX port for ConcourseServer.
     */
    public static final int JMX_PORT = Tools.getJmxPort();

    /**
     * The url used for connecting to the server's JMX service.
     */
    public static final String JMX_SERVICE_URL = "service:jmx:rmi:///jndi/rmi://localhost:"
            + JMX_PORT + "/jmxrmi";

    /**
     * Return a string that contains the dumps for all the storage units (i.e.
     * buffer, primary, secondary, search) identified by {@code id}.
     * 
     * @return the dump string
     */
    @ManagedOperation
    public String dump(String id);

    /**
     * Grant access to the user identified by the combination of
     * {@code username} and {@code password}.
     * 
     * @param username
     * @param password
     */
    public void grant(byte[] username, byte[] password);

    /**
     * Remove the user identified by {@code username}.
     * 
     * @param username
     */
    public void revoke(byte[] username);

    /**
     * Return {@code true} if {@code username} and {@code password} is a valid
     * combination to login to the server for the purpose of performing a
     * managed operation. This method should only be used to authenticate a user
     * for the purpose of performing a single operation.
     * 
     * @param username
     * @param password
     * @return {@code true} if the credentials are valid
     */
    @ManagedOperation
    public boolean login(byte[] username, byte[] password);

    /**
     * JMX tools
     * 
     * @author jnelson
     */
    public static class Tools {

        /**
         * Get the JMX port from {@code conf/concourse.conf}.
         * 
         * @return the JMX port
         */
        public static int getJmxPort() {
            // This method will not work in Windows or any platform that does
            // not have 'grep or 'cut' installed.
            int port = 0;
            try {
                Process process = Runtime
                        .getRuntime()
                        .exec(new String[] { "sh", "-c",
                                "grep jmxremote.port conf/concourse.conf | cut -d'=' -f3" });
                process.waitFor();
                BufferedReader input = new BufferedReader(
                        new InputStreamReader(process.getInputStream()));
                port = Integer.parseInt(input.readLine());
            }
            catch (Exception e) {
                // swallow and just return 0
            }
            return port;
        }
    }

}
