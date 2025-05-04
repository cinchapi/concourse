/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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
package com.cinchapi.concourse.server;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import oshi.SystemInfo;

/**
 * Interface for various system metrics and information.
 *
 * @author Jeff Nelson
 */
public final class Telemetry { 

    /**
     * Returns the {@link Telemetry} instance.
     * 
     * @return the Telemetry instance
     */
    public static Telemetry get() {
        return INSTANCE;
    }

    /**
     * The singleton instance of the Telemetry class.
     */
    private final static Telemetry INSTANCE = new Telemetry();

    /**
     * OSHI handle.
     */
    private final SystemInfo oshi;

    /**
     * Construct a new instance.
     */
    private Telemetry() {
        this.oshi = new SystemInfo();
    }

    /**
     * Returns the amount of heap memory available to the JVM.
     * 
     * @return the available heap memory in bytes
     */
    public long availableHeapMemory() {
        return Runtime.getRuntime().freeMemory();
    }

    /**
     * Returns the amount of available physical memory on the system.
     * 
     * @return the available physical memory in bytes
     */
    public long availableSystemMemory() {
        return oshi.getHardware().getMemory().getAvailable();
    }

    /**
     * Determines if the current process is running in a containerized
     * environment.
     * 
     * @return true if running in a container, false otherwise
     */
    public boolean isContainerized() {
        if(oshi.getOperatingSystem().getVersionInfo().getBuildNumber()
                .toLowerCase().contains("container")) {
            return true;
        }
        if(System.getenv().containsKey("KUBERNETES_SERVICE_HOST")) {
            return true;
        }
        if(System.getenv().containsKey("DOCKER_CONTAINER")) {
            return true;
        }
        if(new File("/.dockerenv").exists()) {
            // Check for the very common /.dockerenv marker
            return true;
        }
        try (BufferedReader reader = new BufferedReader(
                new FileReader("/proc/1/cgroup"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                // Kubernetes pods, Docker, LXC, etc tend to have recognizable
                // paths
                if(line.contains("docker") || line.contains("kubepods")
                        || line.contains("containerd")
                        || line.contains("lxc")) {
                    return true;
                }
            }
        }
        catch (IOException e) {}
        return false;

    }

    /**
     * Determines if swap memory is enabled on the system.
     * 
     * @return true if swap is enabled, false otherwise
     */
    public boolean isSwapEnabled() {
        return oshi.getHardware().getMemory().getVirtualMemory()
                .getSwapTotal() > 0;
    }

    /**
     * Returns the underlying system information object.
     * 
     * @return the SystemInfo instance
     */
    public SystemInfo system() {
        return oshi;
    }

    /**
     * Returns the amount of heap memory reserved by the JVM.
     * 
     * @return the reserved heap memory in bytes
     */
    public long totalHeapMemory() {
        return Runtime.getRuntime().totalMemory();
    }

    /**
     * Returns the total physical memory available on the system.
     * 
     * @return the total physical memory in bytes
     */
    public long totalMemory() {
        return oshi.getHardware().getMemory().getTotal();
    }

    /**
     * Returns the total amount of virtual memory (swap space) on the system.
     * 
     * @return the total virtual memory in bytes
     */
    public long virtualMemory() {
        return oshi.getHardware().getMemory().getVirtualMemory().getSwapTotal();
    }
}
