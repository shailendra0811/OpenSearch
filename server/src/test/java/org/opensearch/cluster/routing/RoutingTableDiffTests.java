/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.Diff;

import static org.hamcrest.Matchers.is;

public class RoutingTableDiffTests extends RoutingTableTests {

    public void testRoutingTableShardsWithState() {
        assertThat(clusterState.routingTable().shardsWithState(ShardRoutingState.UNASSIGNED).size(), is(this.totalNumberOfShards));

        initPrimaries();
        assertThat(
            clusterState.routingTable().shardsWithState(ShardRoutingState.UNASSIGNED).size(),
            is(this.totalNumberOfShards - 2 * this.numberOfShards)
        );
        assertThat(clusterState.routingTable().shardsWithState(ShardRoutingState.INITIALIZING).size(), is(2 * this.numberOfShards));

        startInitializingShards(TEST_INDEX_1);
        assertThat(clusterState.routingTable().shardsWithState(ShardRoutingState.STARTED).size(), is(this.numberOfShards));
        int initializingExpected = this.numberOfShards + this.numberOfShards * this.numberOfReplicas;
        assertThat(clusterState.routingTable().shardsWithState(ShardRoutingState.INITIALIZING).size(), is(initializingExpected));
        assertThat(
            clusterState.routingTable().shardsWithState(ShardRoutingState.UNASSIGNED).size(),
            is(this.totalNumberOfShards - initializingExpected - this.numberOfShards)
        );

        startInitializingShards(TEST_INDEX_2);
        assertThat(clusterState.routingTable().shardsWithState(ShardRoutingState.STARTED).size(), is(2 * this.numberOfShards));
        initializingExpected = 2 * this.numberOfShards * this.numberOfReplicas;
        assertThat(clusterState.routingTable().shardsWithState(ShardRoutingState.INITIALIZING).size(), is(initializingExpected));
        assertThat(
            clusterState.routingTable().shardsWithState(ShardRoutingState.UNASSIGNED).size(),
            is(this.totalNumberOfShards - initializingExpected - 2 * this.numberOfShards)
        );
        ClusterState oldClusterState = clusterState;
        // now start all replicas too
        //startInitializingShards(TEST_INDEX_1);
        clusterState = startRandomInitializingShard(clusterState, ALLOCATION_SERVICE);
        //startInitializingShards(TEST_INDEX_2);
        //assertThat(clusterState.routingTable().shardsWithState(ShardRoutingState.STARTED).size(), is(this.totalNumberOfShards));
        Diff<RoutingTable> diff = clusterState.routingTable().diff(oldClusterState.getRoutingTable());
        Diff<RoutingTable> incrementalDiff = clusterState.routingTable().incrementalDiff(oldClusterState.getRoutingTable());
        RoutingTable newRoutingTable = incrementalDiff.apply(oldClusterState.getRoutingTable());
        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            assertEquals(clusterState.routingTable().version(), newRoutingTable.version());
            assertEquals(indexRoutingTable, newRoutingTable.index(indexRoutingTable.getIndex()));
        }
    }

}
