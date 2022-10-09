/*
 * Copyright (c) 2013-2022 Cinchapi Inc.
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
package com.cinchapi.concourse.server.config;

import java.util.List;

import com.cinchapi.concourse.config.ConcourseServerConfiguration;
import com.google.common.collect.ImmutableList;

/**
 * Container for all cluster configuration.
 *
 * @author Jeff Nelson
 */
public class ConcourseClusterConfiguration {

    /**
     * Return the {@link ConcourseClusterConfiguration} that is parsed from the
     * {@code ConcourseServerConfig source}.
     * 
     * @param source
     * @return the {@link ConcourseClusterConfiguration}
     */
    public static ConcourseClusterConfiguration from(
            ConcourseServerConfiguration source) {
        return new ConcourseClusterConfiguration(source);
    }

    /**
     * Return the defaults.
     */
    public final static ConcourseClusterConfiguration DEFAULTS = new ConcourseClusterConfiguration(
            null) {

        @Override
        public boolean isEnabled() {
            return false;
        }
    };

    /**
     * Config source.
     */
    private final ConcourseServerConfiguration source;

    /**
     * Construct a new instance.
     * 
     * @param source
     */
    private ConcourseClusterConfiguration(ConcourseServerConfiguration source) {
        this.source = source;
    }

    /**
     * Return {@code true} if cluster support is enabled, based on the
     * configuration.
     */
    public boolean isEnabled() {
        return !nodes().isEmpty();
    }

    /**
     * Return the list of server addresses for each node in the cluster.
     * 
     * @return the cluster nodes
     */
    public List<String> nodes() {
        return source.getOrDefault("cluster.nodes", ImmutableList.of());
    }

    /**
     * Return the replication factor.
     * 
     * @return the replication factor
     */
    public int replicationFactor() {
        int size = nodes().size();
        int defaultReplicationFactor;
        if(size >= 4) {
            defaultReplicationFactor = 3;
        }
        else if(size > 1) {
            defaultReplicationFactor = 2;
        }
        else {
            defaultReplicationFactor = 1;
        }
        return source.getIgnoreCaseFormatOrDefault("cluster.replication_factor",
                defaultReplicationFactor);
    }

}
