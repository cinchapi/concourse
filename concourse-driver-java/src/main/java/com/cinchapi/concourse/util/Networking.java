/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse.util;

import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.SocketException;
import java.net.URL;
import java.net.UnknownHostException;

import com.google.common.base.Throwables;

/**
 * Utilities for dealing with networking.
 * 
 * @author Jeff Nelson
 */
public final class Networking {

    /**
     * Return the <em>companion</em> port for the specified one. The companion
     * port is one between {@value #MIN_PORT} and {@value #MAX_PORT} that is
     * guaranteed to be different from {@code port}.
     * 
     * @param port
     * @return the companion port
     */
    public static int getCompanionPort(int port) {
        return port < PORT_RANGE ? MIN_PORT + port : (port % PORT_RANGE)
                + MIN_PORT;
    }

    /**
     * Return the <em>companion</em> port for the specified one. The companion
     * port is one between {@value #MIN_PORT} and {@value #MAX_PORT} that is
     * guaranteed to be different from {@code port}.
     * 
     * @param port
     * @param rounds
     * @return the companion port
     */
    public static int getCompanionPort(int port, int rounds) {
        for (int i = 0; i < rounds; i++) {
            port = getCompanionPort(port);
        }
        return port;
    }

    /**
     * Get the accessible IP Address for the {@code host}.
     * 
     * @param host
     * @return the ip address
     */
    public static String getIpAddress(String host) {
        try {
            if(host.equalsIgnoreCase("localhost")
                    || host.equalsIgnoreCase("127.0.0.1")
                    || host.equalsIgnoreCase("0.0.0.0")) {
                return InetAddress.getLocalHost().getHostAddress();
            }
            if(host.startsWith("http") || host.startsWith("https")) {
                host = new URL(host).getHost();
            }
            return InetAddress.getByName(host).getHostAddress();
        }
        catch (UnknownHostException | MalformedURLException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Get an open port on the local machine in the port range between
     * {@value #MIN_PORT} and {@value #MAX_PORT}.
     * 
     * @return the port
     */
    public static int getOpenPort() {
        int port = RAND.nextInt(MIN_PORT) + (PORT_RANGE);
        return isOpenPort(port) ? port : getOpenPort();
    }

    /**
     * Return {@code true} if the {@code host} belongs to the local machine.
     * 
     * @param host
     * @return {@code true} if the {@code host} is local
     */
    public static boolean isLocalHost(String host) {
        try {
            InetAddress inet = InetAddress.getByName(host);
            if(inet.isAnyLocalAddress() || inet.isLoopbackAddress()) {
                return true;
            }
            else {
                return NetworkInterface.getByInetAddress(inet) != null;
            }
        }
        catch (SocketException | UnknownHostException e) {
            return false;
        }
    }

    /**
     * Return {@code true} if the {@code port} is available on the local
     * machine.
     * 
     * @param port
     * @return {@code true} if the port is available
     */
    private static boolean isOpenPort(int port) {
        try {
            new ServerSocket(port).close();
            return true;
        }
        catch (SocketException e) {
            return false;
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Regex to match an IP Address.
     */
    public static final String IP_REGEX = "(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)";

    /**
     * The max port number that can be assigned from the
     * {@link #getCompanionPort(int)} method.
     */
    private static final int MAX_PORT = 65535;

    /**
     * The min port number that can be assigned from the
     * {@link #getCompanionPort(int)} method.
     */
    private static final int MIN_PORT = 49152;

    /**
     * The number of available ports that can be considered "companion" ports.
     */
    private static final int PORT_RANGE = MAX_PORT - MIN_PORT;

    /**
     * Class wide random number generator
     */
    private static final java.util.Random RAND = new java.util.Random();

    private Networking() {/* noop */}

}
