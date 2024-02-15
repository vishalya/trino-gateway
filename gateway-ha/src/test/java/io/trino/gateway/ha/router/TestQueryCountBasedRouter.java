/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.gateway.ha.router;

import com.google.common.collect.ImmutableList;
import io.trino.gateway.ha.clustermonitor.ClusterStats;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class TestQueryCountBasedRouter
{
    static final String BACKEND_URL_1 = "http://c1";
    static final String BACKEND_URL_2 = "http://c2";
    static final String BACKEND_URL_3 = "http://c3";
    static final String BACKEND_URL_4 = "http://c4";
    static final String BACKEND_URL_5 = "http://c5";
    static final String BACKEND_URL_UNHEALTHY = "http://c-unhealthy";

    static final int LEAST_QUEUED_COUNT = 1;
    static final int SAME_QUERY_COUNT = 5;
    QueryCountBasedRouter queryCountBasedRouter;
    ImmutableList<ClusterStats> clusters;

    TestQueryCountBasedRouter()
    {
        queryCountBasedRouter = null;
        clusters = null;
    }

    // Helper function to generate the ClusterStat list
    private static ImmutableList getClusterStatsList(String routingGroup)
    {
        ImmutableList.Builder<ClusterStats> clustersBuilder = new ImmutableList.Builder();
        // Set Cluster1 stats
        {
            ClusterStats cluster = new ClusterStats();
            cluster.setClusterId("c1");
            cluster.setProxyTo(BACKEND_URL_1);
            cluster.setHealthy(true);
            cluster.setRoutingGroup(routingGroup);
            cluster.setRunningQueryCount(50);
            cluster.setQueuedQueryCount(SAME_QUERY_COUNT);
            cluster.setUserQueuedCount(new HashMap<>(Map.of("u1", 5, "u2", 10, "u3", 2)));
            clustersBuilder.add(cluster);
        }
        // Set Cluster2 stats
        {
            ClusterStats cluster = new ClusterStats();
            cluster.setClusterId("c2");
            cluster.setProxyTo(BACKEND_URL_2);
            cluster.setHealthy(true);
            cluster.setRoutingGroup(routingGroup);
            cluster.setRunningQueryCount(51);
            cluster.setQueuedQueryCount(SAME_QUERY_COUNT);

            HashMap<String, Integer> userToQueueCount2 = new HashMap<String, Integer>();
            cluster.setUserQueuedCount(userToQueueCount2);
            cluster.setUserQueuedCount(new HashMap<>(Map.of("u1", 5, "u2", 1, "u3", 12)));

            clustersBuilder.add(cluster);
        }
        // Set Cluster3 stats with the least no of queries running
        {
            ClusterStats cluster = new ClusterStats();
            cluster.setClusterId("c3");
            cluster.setProxyTo(BACKEND_URL_3);
            cluster.setHealthy(true);
            cluster.setRoutingGroup(routingGroup);
            cluster.setRunningQueryCount(5);
            cluster.setQueuedQueryCount(SAME_QUERY_COUNT);

            HashMap<String, Integer> userToQueueCount3 = new HashMap<String, Integer>();
            cluster.setUserQueuedCount(userToQueueCount3);
            cluster.setUserQueuedCount(new HashMap<>(Map.of("u1", 5, "u2", 2, "u3", 6)));

            clustersBuilder.add(cluster);
        }
        // cluster - unhealthy one
        {
            ClusterStats cluster = new ClusterStats();
            cluster.setProxyTo("http://c-unhealthy");
            cluster.setClusterId("c-unhealthy");
            cluster.setHealthy(false); //This cluster should never show up to route
            cluster.setRoutingGroup(routingGroup);
            cluster.setRunningQueryCount(5);
            cluster.setQueuedQueryCount(SAME_QUERY_COUNT);
            cluster.setUserQueuedCount(new HashMap<String, Integer>());

            clustersBuilder.add(cluster);
        }
        // cluster - unhealthy one, no stats
        {
            ClusterStats cluster = new ClusterStats();
            cluster.setProxyTo("http://c-unhealthy2");
            cluster.setClusterId("c-unhealthy2");
            cluster.setHealthy(false); //This cluster should never show up to route

            clustersBuilder.add(cluster);
        }
        // cluster - it's messed up - healthy but no stats
        {
            ClusterStats cluster = new ClusterStats();
            cluster.setProxyTo("http://c-messed-up");
            cluster.setClusterId("c-unhealthy2");
            //This is a scenrio when, something is really wrong
            //We just get the cluster state as health but no stats
            cluster.setHealthy(true);
            clustersBuilder.add(cluster);
        }

        return clustersBuilder.build();
    }

    static ClusterStats getClusterWithNoUserQueueAndMinQueueCount()
    {
        ClusterStats cluster = new ClusterStats();
        cluster.setClusterId("c-Minimal-Queue");
        cluster.setProxyTo(BACKEND_URL_4);
        cluster.setHealthy(true);
        cluster.setRoutingGroup("adhoc");
        cluster.setRunningQueryCount(5);
        cluster.setQueuedQueryCount(LEAST_QUEUED_COUNT);
        cluster.setUserQueuedCount(new HashMap<String, Integer>());
        return cluster;
    }

    static ClusterStats getClusterWithMinRunnningQueries()
    {
        ClusterStats cluster = new ClusterStats();
        cluster.setClusterId("c-Minimal-Running");
        cluster.setProxyTo(BACKEND_URL_5);
        cluster.setHealthy(true);
        cluster.setRoutingGroup("adhoc");
        cluster.setRunningQueryCount(1);
        cluster.setQueuedQueryCount(LEAST_QUEUED_COUNT);
        cluster.setUserQueuedCount(new HashMap<String, Integer>());
        return cluster;
    }

    @BeforeEach
    public void init()
    {
        //Have a adoc and an etl routing groups - 2 sets of clusters
        clusters = new ImmutableList.Builder()
                .addAll(getClusterStatsList("adhoc"))
                .addAll(getClusterStatsList("etl"))
                .build();

        queryCountBasedRouter = new QueryCountBasedRouter(null, null);
        queryCountBasedRouter.upateBackEndStats(clusters);
    }

    @Test
    public void testUserWithSameNoOfQueuedQueries()
    {
        // The user u1 has same number of queries queued on each cluster
        // The query needs to be routed to cluster with least number of queries running
        String proxyTo = queryCountBasedRouter.provideBackendForRoutingGroup("etl", "u1");
        assertEquals(BACKEND_URL_3, proxyTo);
        assertNotEquals(BACKEND_URL_UNHEALTHY, proxyTo);

        //After the above code is run, c3 cluster has 6 queued queries
        //c1, c2 cluster will be with same original number of queued queries i.e. 5 each
        //The next query should go to the c1 cluster, as it would have less number of cluster wide
        ClusterStats c3Stats = clusters.stream()
                .filter(c -> c.getClusterId().equals("c3") &&
                        c.getRoutingGroup().equals("etl"))
                .findAny().orElseThrow();
        assertEquals(6, c3Stats.getUserQueuedCount().getOrDefault("u1", 0));

        proxyTo = queryCountBasedRouter.provideBackendForRoutingGroup("etl", "u1");
        assertEquals(BACKEND_URL_1, proxyTo);
        assertNotEquals(BACKEND_URL_UNHEALTHY, proxyTo);
    }

    @Test
    public void testUserWithDifferentQueueLengthUser1()
    {
        // The user u2 has different number of queries queued on each cluster
        // The query needs to be routed to cluster with least number of queued for that user
        String proxyTo = queryCountBasedRouter.provideAdhocBackend("u2");
        assertEquals(BACKEND_URL_2, proxyTo);
        assertNotEquals(BACKEND_URL_UNHEALTHY, proxyTo);
    }

    @Test
    public void testUserWithDifferentQueueLengthUser2()
    {
        String proxyTo = queryCountBasedRouter.provideAdhocBackend("u3");
        assertEquals(BACKEND_URL_1, proxyTo);
        assertNotEquals(BACKEND_URL_UNHEALTHY, proxyTo);
    }

    @Test
    public void testUserWithNoQueuedQueries()
    {
        String proxyTo = queryCountBasedRouter.provideAdhocBackend("u101");
        assertEquals(BACKEND_URL_3, proxyTo);
    }

    @Test
    public void testAdhocRoutingGroupFailOver()
    {
        // The ETL routing group doesn't exist
        String proxyTo = queryCountBasedRouter.provideBackendForRoutingGroup("NonExisting", "u1");
        assertEquals(BACKEND_URL_3, proxyTo);
        assertNotEquals(BACKEND_URL_UNHEALTHY, proxyTo);
    }

    @Test
    public void testClusterWithLeastQueueCount()
    {
        // Add a cluster with minimal queuelength
        clusters = new ImmutableList.Builder()
                .addAll(clusters)
                .add(getClusterWithNoUserQueueAndMinQueueCount())
                .build();
        queryCountBasedRouter.upateBackEndStats(clusters);

        String proxyTo = queryCountBasedRouter.provideBackendForRoutingGroup("NonExisting", "u1");
        assertEquals(BACKEND_URL_4, proxyTo);
        assertNotEquals(BACKEND_URL_UNHEALTHY, proxyTo);
    }

    @Test
    public void testClusterWithLeastRunningCount()
    {
        // Add a cluster with minimal queuelength
        clusters = new ImmutableList.Builder()
                .addAll(clusters)
                .add(getClusterWithNoUserQueueAndMinQueueCount())
                .add(getClusterWithMinRunnningQueries())
                .build();

        queryCountBasedRouter.upateBackEndStats(clusters);

        String proxyTo = queryCountBasedRouter.provideBackendForRoutingGroup("NonExisting", "u1");
        assertEquals(BACKEND_URL_5, proxyTo);
        assertNotEquals(BACKEND_URL_UNHEALTHY, proxyTo);
    }
}
