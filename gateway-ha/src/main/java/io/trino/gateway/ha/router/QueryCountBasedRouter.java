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

import io.trino.gateway.ha.clustermonitor.ClusterStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class QueryCountBasedRouter
            extends HaRoutingManager
{
    private static final Logger log = LoggerFactory.getLogger(QueryCountBasedRouter.class);
    private ArrayList<ClusterStats> clusterStats;
    private static final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    public QueryCountBasedRouter(GatewayBackendManager gatewayBackendManager,
                                 QueryHistoryManager queryHistoryManager)
    {
        super(gatewayBackendManager, queryHistoryManager);
        clusterStats = new ArrayList<>();
    }

    // We sort and find the backend based on the individual user's count of the queued queries
    // first, in case user doesn't have any queries queued we use the cluster wide stats
    //
    // First filter the list of clusters for a particular routing group
    //
    // If a user has queued queries, then find a cluster with the least number of QUEUED queries
    // for that user.
    //
    // If a user's QUEUED query count is the same on every cluster then go with a cluster with
    // cluster wide stats.
    //
    // Find a cluster with the least number of QUEUED queries, if there are the same number of
    // queries queued, then compare the number of running queries.
    //
    // After a query is routed, we need to update the stats for that cluster until we received the
    // updated stats for all the clusters.
    // if a user has queries queued then we assume that the routed query will be also queued or
    // else we assume it would be scheduled immediately and we increment the stats for the running
    // queries

    private Optional<ClusterStats> getClusterToRoute(String user, String routingGroup)
    {
        List<ClusterStats> filteredList = null;
        Lock readLock = lock.readLock();
        try {
            readLock.lock();
            log.debug("sorting cluster stats for {} {}", user, routingGroup);
            filteredList = clusterStats.stream()
                .filter(stats -> stats.healthy())
                .filter(stats -> routingGroup.equals(stats.routingGroup()))
                .collect(Collectors.toList());
        }
        finally {
            readLock.unlock();
        }

        if (filteredList.isEmpty()) {
            return Optional.empty();
        }

        Collections.sort(filteredList, new Comparator<ClusterStats>()
        {
            public int compare(ClusterStats lhs, ClusterStats rhs)
            {
                // First check if the user has any queries queued
                int compareUserQueue = Integer.compare(lhs.userQueuedCount().getOrDefault(user, 0),
                        rhs.userQueuedCount().getOrDefault(user, 0));

                if (compareUserQueue != 0) {
                    return compareUserQueue;
                }

                int compareClusterQueue = Integer.compare(lhs.queuedQueryCount(),
                                                            rhs.queuedQueryCount());
                if (compareClusterQueue != 0) {
                    return compareClusterQueue;
                }

                // If the user has equal number of queries queued then see which cluster
                // has less number of queries running and route it accordingly
                return Integer.compare(lhs.runningQueryCount(),
                    rhs.runningQueryCount());
            }
        });
        // The first cluster is right one
        return Optional.of(filteredList.get(0));
    }

    private Optional<String> getBackendForRoutingGroup(String routingGroup, String user)
    {
        Optional<ClusterStats> cluster = getClusterToRoute(user, routingGroup);
        if (cluster.isPresent()) {
            cluster.orElseThrow().updateLocalStats(user);
            return Optional.of(cluster.orElseThrow().proxyTo());
        }
        return Optional.empty();
    }

    @Override
    public String provideAdhocBackend(String user)
    {
        return getBackendForRoutingGroup("adhoc", user).orElseThrow();
    }

    @Override
    public String provideBackendForRoutingGroup(String routingGroup, String user)
    {
        Optional<String> proxyUrl = getBackendForRoutingGroup(routingGroup, user);
        if (proxyUrl.isPresent()) {
            return proxyUrl.orElseThrow();
        }
        return provideAdhocBackend(user);
    }

    @Override
    public void upateBackEndStats(List<ClusterStats> stats)
    {
        Lock writeLock = lock.writeLock();
        try {
            writeLock.lock();
            clusterStats.clear();
            clusterStats.addAll(stats);
        }
        finally {
            writeLock.unlock();
        }
    }
}
