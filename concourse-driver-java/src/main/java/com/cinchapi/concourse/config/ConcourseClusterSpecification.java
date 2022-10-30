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
package com.cinchapi.concourse.config;

import java.util.List;

import com.cinchapi.common.collect.Association;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * The specification for a Concourse distributed cluster.
 *
 * @author Jeff Nelson
 */
public class ConcourseClusterSpecification {

    /**
     * Return a {@link ConcourseClusterSpecification} that contains the default
     * values.
     * 
     * @return the {@link ConcourseClusterSpecification}.
     */
    public static ConcourseClusterSpecification defaults() {
        return new ConcourseClusterSpecification(Association.of());
    }

    /**
     * Return a {@link ConcourseClusterSpecification} that is sourced from the
     * {@code config}.
     * 
     * @param config
     * @return the {@link ConcourseClusterSpecification}.
     */
    public static ConcourseClusterSpecification from(
            ConcourseServerConfiguration config) {
        Association spec = Association.of(config.get(CLUSTER));
        return new ConcourseClusterSpecification(spec);
    }

    /**
     * The specification for a cluster that is not currently and will not become
     * {@link #isDefined() defined}.
     */
    public static final ConcourseClusterSpecification UNDEFINED = new ConcourseClusterSpecification(
            Association.of(ImmutableMap.of())) {

        @Override
        public boolean isDefined() {
            return false;
        }
    };

    /**
     * The config key for the overall cluster.
     */
    private static final String CLUSTER = "cluster";

    /**
     * The config key for the list of nodes.
     */
    private static final String NODES = "node";

    /**
     * The config key for the replication factor.
     */
    private static final String REPLICATION_FACTOR = "replication_factor";

    /**
     * The specification values.
     */
    private final Association spec;

    /**
     * Construct a new instance.
     * 
     * @param source
     */
    private ConcourseClusterSpecification(Association spec) {
        this.spec = spec;
    }

    /**
     * Return {@code true} if, based on the specification, a cluster is defined.
     * 
     * @return {@code true} if a cluster is defined
     */
    public boolean isDefined() {
        return !nodes().isEmpty();
    }

    /**
     * Return the list of server addresses for each node in the cluster.
     * 
     * @return the cluster nodes
     */
    public List<String> nodes() {
        return spec.fetchOrDefault(NODES, ImmutableList.of());
    }

    /**
     * Set the nodes in the cluster
     * 
     * @param nodes
     */
    public void nodes(List<String> nodes) {
        spec.set(NODES, nodes);
    }

    /**
     * Publish this {@link ConcourseClusterSpecification} to the {@code config}.
     * <p>
     * NOTE: This will entirely overwrite any cluster specification that already
     * exists in {@code config}.
     * </p>
     * 
     * @param config
     */
    public void publish(ConcourseServerConfiguration config) {
        config.set(CLUSTER, spec);
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
        return spec.fetchOrDefault(REPLICATION_FACTOR,
                defaultReplicationFactor);
    }

    /**
     * Set the replication factor
     * 
     * @param replicationFactor
     */
    public void replicationFactor(int replicationFactor) {
        spec.set(REPLICATION_FACTOR, replicationFactor);
    }

}
