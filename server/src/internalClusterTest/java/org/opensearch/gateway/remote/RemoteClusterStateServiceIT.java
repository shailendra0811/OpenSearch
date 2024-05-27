/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.settings.Settings;
import org.opensearch.discovery.DiscoveryStats;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_CLEANUP_INTERVAL_SETTING;
import static org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING;
import static org.opensearch.gateway.remote.RemoteClusterStateService.RETAINED_MANIFESTS;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.DELIMITER;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.METADATA_FILE_PREFIX;
import static org.opensearch.gateway.remote.RemoteGlobalMetadataManager.COORDINATION_METADATA;
import static org.opensearch.gateway.remote.RemoteGlobalMetadataManager.CUSTOM_METADATA;
import static org.opensearch.gateway.remote.RemoteGlobalMetadataManager.SETTING_METADATA;
import static org.opensearch.gateway.remote.RemoteGlobalMetadataManager.TEMPLATES_METADATA;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteClusterStateServiceIT extends RemoteStoreBaseIntegTestCase {

    private static String INDEX_NAME = "test-index";

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put(REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true).build();
    }

    private void prepareCluster(int numClusterManagerNodes, int numDataOnlyNodes, String indices, int replicaCount, int shardCount) {
        internalCluster().startClusterManagerOnlyNodes(numClusterManagerNodes);
        internalCluster().startDataOnlyNodes(numDataOnlyNodes);
        for (String index : indices.split(",")) {
            createIndex(index, remoteStoreIndexSettings(replicaCount, shardCount));
            ensureYellowAndNoInitializingShards(index);
            ensureGreen(index);
        }
    }

    private Map<String, Long> initialTestSetup(int shardCount, int replicaCount, int dataNodeCount, int clusterManagerNodeCount) {
        prepareCluster(clusterManagerNodeCount, dataNodeCount, INDEX_NAME, replicaCount, shardCount);
        Map<String, Long> indexStats = indexData(1, false, INDEX_NAME);
        assertEquals(shardCount * (replicaCount + 1), getNumShards(INDEX_NAME).totalNumShards);
        ensureGreen(INDEX_NAME);
        return indexStats;
    }

    public void testRemoteCleanupTaskUpdated() {
        int shardCount = randomIntBetween(1, 2);
        int replicaCount = 1;
        int dataNodeCount = shardCount * (replicaCount + 1);
        int clusterManagerNodeCount = 1;

        initialTestSetup(shardCount, replicaCount, dataNodeCount, clusterManagerNodeCount);
        RemoteClusterStateService remoteClusterStateService = internalCluster().getClusterManagerNodeInstance(
            RemoteClusterStateService.class
        );

        assertEquals(5, remoteClusterStateService.getStaleFileDeletionTask().getInterval().getMinutes());
        assertTrue(remoteClusterStateService.getStaleFileDeletionTask().isScheduled());

        // now disable
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put(REMOTE_CLUSTER_STATE_CLEANUP_INTERVAL_SETTING.getKey(), -1))
            .get();

        assertEquals(-1, remoteClusterStateService.getStaleFileDeletionTask().getInterval().getMillis());
        assertFalse(remoteClusterStateService.getStaleFileDeletionTask().isScheduled());

        // now set Clean up interval to 1 min
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put(REMOTE_CLUSTER_STATE_CLEANUP_INTERVAL_SETTING.getKey(), "1m"))
            .get();
        assertEquals(1, remoteClusterStateService.getStaleFileDeletionTask().getInterval().getMinutes());
    }

    public void testRemoteCleanupOnlyAfter10Updates() throws Exception {
        int shardCount = randomIntBetween(1, 2);
        int replicaCount = 1;
        int dataNodeCount = shardCount * (replicaCount + 1);
        int clusterManagerNodeCount = 1;

        initialTestSetup(shardCount, replicaCount, dataNodeCount, clusterManagerNodeCount);
        RemoteClusterStateService remoteClusterStateService = internalCluster().getClusterManagerNodeInstance(
            RemoteClusterStateService.class
        );

        // set cleanup interval to 1 min
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put(REMOTE_CLUSTER_STATE_CLEANUP_INTERVAL_SETTING.getKey(), "1m"))
            .get();

        replicaCount = updateReplicaCountNTimes(9, replicaCount);

        RepositoriesService repositoriesService = internalCluster().getClusterManagerNodeInstance(RepositoriesService.class);

        BlobStoreRepository repository = (BlobStoreRepository) repositoriesService.repository(REPOSITORY_NAME);
        BlobPath baseMetadataPath = repository.basePath()
            .add(
                Base64.getUrlEncoder()
                    .withoutPadding()
                    .encodeToString(getClusterState().getClusterName().value().getBytes(StandardCharsets.UTF_8))
            )
            .add("cluster-state")
            .add(getClusterState().metadata().clusterUUID());
        BlobPath manifestContainerPath = baseMetadataPath.add("manifest");

        assertBusy(() -> {
            assertEquals(
                RETAINED_MANIFESTS - 1,
                repository.blobStore().blobContainer(manifestContainerPath).listBlobsByPrefix("manifest").size()
            );
        }, 1, TimeUnit.MINUTES);

        replicaCount = updateReplicaCountNTimes(8, replicaCount);

        // wait for 1 min, to ensure that clean up task ran and didn't clean up stale files because it was less than 10
        Thread.sleep(60000);
        assertNotEquals(
            RETAINED_MANIFESTS - 1,
            repository.blobStore().blobContainer(manifestContainerPath).listBlobsByPrefix("manifest").size()
        );

        // Do 2 more updates, now since the total successful state changes are more than 10, stale files will be cleaned up
        replicaCount = updateReplicaCountNTimes(2, replicaCount);

        assertBusy(() -> {
            assertEquals(
                RETAINED_MANIFESTS - 1,
                repository.blobStore().blobContainer(manifestContainerPath).listBlobsByPrefix("manifest").size()
            );
        }, 1, TimeUnit.MINUTES);

        Map<String, IndexMetadata> indexMetadataMap = remoteClusterStateService.getLatestClusterState(
            cluster().getClusterName(),
            getClusterState().metadata().clusterUUID(),
            false
        ).getMetadata().getIndices();
        assertEquals(replicaCount, indexMetadataMap.values().stream().findFirst().get().getNumberOfReplicas());
        assertEquals(shardCount, indexMetadataMap.values().stream().findFirst().get().getNumberOfShards());
    }

    public void testRemoteStateStats() {
        int shardCount = randomIntBetween(1, 2);
        int replicaCount = 1;
        int dataNodeCount = shardCount * (replicaCount + 1);
        int clusterManagerNodeCount = 1;
        prepareCluster(clusterManagerNodeCount, dataNodeCount, INDEX_NAME, replicaCount, shardCount);
        String clusterManagerNode = internalCluster().getClusterManagerName();
        String dataNode = internalCluster().getDataNodeNames().stream().collect(Collectors.toList()).get(0);

        // Fetch _nodes/stats
        NodesStatsResponse nodesStatsResponse = client().admin()
            .cluster()
            .prepareNodesStats(clusterManagerNode)
            .addMetric(NodesStatsRequest.Metric.DISCOVERY.metricName())
            .get();

        // assert cluster state stats
        assertClusterManagerClusterStateStats(nodesStatsResponse);

        NodesStatsResponse nodesStatsResponseDataNode = client().admin()
            .cluster()
            .prepareNodesStats(dataNode)
            .addMetric(NodesStatsRequest.Metric.DISCOVERY.metricName())
            .get();
        // assert cluster state stats for data node
        DiscoveryStats dataNodeDiscoveryStats = nodesStatsResponseDataNode.getNodes().get(0).getDiscoveryStats();
        assertNotNull(dataNodeDiscoveryStats.getClusterStateStats());
        assertEquals(0, dataNodeDiscoveryStats.getClusterStateStats().getUpdateSuccess());

        // call nodes/stats with nodeId filter
        NodesStatsResponse nodesStatsNodeIdFilterResponse = client().admin()
            .cluster()
            .prepareNodesStats(dataNode)
            .addMetric(NodesStatsRequest.Metric.DISCOVERY.metricName())
            .setNodesIds(clusterManagerNode)
            .get();

        assertClusterManagerClusterStateStats(nodesStatsNodeIdFilterResponse);
    }

    private void assertClusterManagerClusterStateStats(NodesStatsResponse nodesStatsResponse) {
        // assert cluster state stats
        DiscoveryStats discoveryStats = nodesStatsResponse.getNodes().get(0).getDiscoveryStats();

        assertNotNull(discoveryStats.getClusterStateStats());
        assertTrue(discoveryStats.getClusterStateStats().getUpdateSuccess() > 1);
        assertEquals(0, discoveryStats.getClusterStateStats().getUpdateFailed());
        assertTrue(discoveryStats.getClusterStateStats().getUpdateTotalTimeInMillis() > 0);
        // assert remote state stats
        assertTrue(discoveryStats.getClusterStateStats().getPersistenceStats().get(0).getSuccessCount() > 1);
        assertEquals(0, discoveryStats.getClusterStateStats().getPersistenceStats().get(0).getFailedCount());
        assertTrue(discoveryStats.getClusterStateStats().getPersistenceStats().get(0).getTotalTimeInMillis() > 0);
    }

    public void testRemoteStateStatsFromAllNodes() {
        int shardCount = randomIntBetween(1, 5);
        int replicaCount = 1;
        int dataNodeCount = shardCount * (replicaCount + 1);
        int clusterManagerNodeCount = 3;
        prepareCluster(clusterManagerNodeCount, dataNodeCount, INDEX_NAME, replicaCount, shardCount);
        String[] allNodes = internalCluster().getNodeNames();
        // call _nodes/stats/discovery from all the nodes
        for (String node : allNodes) {
            NodesStatsResponse nodesStatsResponse = client().admin()
                .cluster()
                .prepareNodesStats(node)
                .addMetric(NodesStatsRequest.Metric.DISCOVERY.metricName())
                .get();
            validateNodesStatsResponse(nodesStatsResponse);
        }

        // call _nodes/stats/discovery from all the nodes with random nodeId filter
        for (String node : allNodes) {
            NodesStatsResponse nodesStatsResponse = client().admin()
                .cluster()
                .prepareNodesStats(node)
                .addMetric(NodesStatsRequest.Metric.DISCOVERY.metricName())
                .setNodesIds(allNodes[randomIntBetween(0, allNodes.length - 1)])
                .get();
            validateNodesStatsResponse(nodesStatsResponse);
        }
    }

    public void testRemoteClusterStateMetadataSplit() throws IOException {
        initialTestSetup(1, 0, 1, 1);

        RemoteClusterStateService remoteClusterStateService = internalCluster().getClusterManagerNodeInstance(
            RemoteClusterStateService.class
        );
        RepositoriesService repositoriesService = internalCluster().getClusterManagerNodeInstance(RepositoriesService.class);
        BlobStoreRepository repository = (BlobStoreRepository) repositoriesService.repository(REPOSITORY_NAME);
        BlobPath globalMetadataPath = repository.basePath()
            .add(
                Base64.getUrlEncoder()
                    .withoutPadding()
                    .encodeToString(getClusterState().getClusterName().value().getBytes(StandardCharsets.UTF_8))
            )
            .add("cluster-state")
            .add(getClusterState().metadata().clusterUUID())
            .add("global-metadata");

        Map<String, Integer> metadataFiles = repository.blobStore()
            .blobContainer(globalMetadataPath)
            .listBlobs()
            .keySet()
            .stream()
            .map(fileName -> {
                logger.info(fileName);
                return fileName.split(DELIMITER)[0];
            })
            .collect(Collectors.toMap(Function.identity(), key -> 1, Integer::sum));

        assertTrue(metadataFiles.containsKey(COORDINATION_METADATA));
        assertEquals(1, (int) metadataFiles.get(COORDINATION_METADATA));
        assertTrue(metadataFiles.containsKey(SETTING_METADATA));
        assertEquals(1, (int) metadataFiles.get(SETTING_METADATA));
        assertTrue(metadataFiles.containsKey(TEMPLATES_METADATA));
        assertEquals(1, (int) metadataFiles.get(TEMPLATES_METADATA));
        assertTrue(metadataFiles.keySet().stream().anyMatch(key -> key.startsWith(CUSTOM_METADATA)));
        assertFalse(metadataFiles.containsKey(METADATA_FILE_PREFIX));
    }

    private void validateNodesStatsResponse(NodesStatsResponse nodesStatsResponse) {
        // _nodes/stats/discovery must never fail due to any exception
        assertFalse(nodesStatsResponse.toString().contains("exception"));
        assertNotNull(nodesStatsResponse.getNodes());
        assertNotNull(nodesStatsResponse.getNodes().get(0));
        assertNotNull(nodesStatsResponse.getNodes().get(0).getDiscoveryStats());
    }

    private void setReplicaCount(int replicaCount) {
        client().admin()
            .indices()
            .prepareUpdateSettings(INDEX_NAME)
            .setSettings(Settings.builder().put(SETTING_NUMBER_OF_REPLICAS, replicaCount))
            .get();
    }

    private int updateReplicaCountNTimes(int n, int initialCount) {
        int newReplicaCount = randomIntBetween(0, 3);
        ;
        for (int i = 0; i < n; i++) {
            while (newReplicaCount == initialCount) {
                newReplicaCount = randomIntBetween(0, 3);
            }
            setReplicaCount(newReplicaCount);
            initialCount = newReplicaCount;
        }
        return newReplicaCount;
    }
}
