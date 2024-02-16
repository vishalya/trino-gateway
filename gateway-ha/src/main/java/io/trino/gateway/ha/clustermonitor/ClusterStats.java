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
package io.trino.gateway.ha.clustermonitor;

import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ClusterStats
{
    private int runningQueryCount;
    private int queuedQueryCount;
    private int blockedQueryCount;
    private int numWorkerNodes;
    private boolean healthy;
    private String clusterId;
    private String proxyTo;
    private String externalUrl;
    private String routingGroup;
    private Map<String, Integer> userQueuedCount;

    public ClusterStats()
    {
        userQueuedCount = new HashMap<>();
    }

    public int runningQueryCount()
    {
        return this.runningQueryCount;
    }

    public void runningQueryCount(int runningQueryCount)
    {
        this.runningQueryCount = runningQueryCount;
    }

    public int queuedQueryCount()
    {
        return this.queuedQueryCount;
    }

    public void queuedQueryCount(int queuedQueryCount)
    {
        this.queuedQueryCount = queuedQueryCount;
    }

    public int numWorkerNodes()
    {
        return this.numWorkerNodes;
    }

    public void numWorkerNodes(int numWorkerNodes)
    {
        this.numWorkerNodes = numWorkerNodes;
    }

    public boolean healthy()
    {
        return this.healthy;
    }

    public void healthy(boolean healthy)
    {
        this.healthy = healthy;
    }

    public String clusterId()
    {
        return this.clusterId;
    }

    public void clusterId(String clusterId)
    {
        this.clusterId = clusterId;
    }

    public String proxyTo()
    {
        return this.proxyTo;
    }

    public void proxyTo(String proxyTo)
    {
        this.proxyTo = proxyTo;
    }

    public String externalUrl()
    {
        return this.externalUrl;
    }

    public void externalUrl(String externalUrl)
    {
        this.externalUrl = externalUrl;
    }

    public String routingGroup()
    {
        return this.routingGroup;
    }

    public void routingGroup(String routingGroup)
    {
        this.routingGroup = routingGroup;
    }

    public Map<String, Integer> userQueuedCount()
    {
        return this.userQueuedCount;
    }

    public void userQueuedCount(Map<String, Integer> userQueuedCount)
    {
        this.userQueuedCount = userQueuedCount;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ClusterStats stats = (ClusterStats) o;
        return runningQueryCount == stats.runningQueryCount &&
                queuedQueryCount == stats.queuedQueryCount &&
                blockedQueryCount == stats.blockedQueryCount &&
                numWorkerNodes == stats.numWorkerNodes &&
                healthy == stats.healthy &&
                Objects.equals(clusterId, stats.clusterId) &&
                Objects.equals(proxyTo, stats.proxyTo) &&
                Objects.equals(externalUrl, stats.externalUrl) &&
                Objects.equals(routingGroup, stats.routingGroup) &&
                Objects.equals(userQueuedCount, stats.userQueuedCount);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(runningQueryCount, queuedQueryCount, blockedQueryCount, numWorkerNodes, healthy, clusterId, proxyTo, externalUrl, routingGroup, userQueuedCount);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("runningQueryCount", runningQueryCount)
                .add("queuedQueryCount", queuedQueryCount)
                .add("blockedQueryCount", blockedQueryCount)
                .add("numWorkerNodes", numWorkerNodes)
                .add("healthy", healthy)
                .add("clusterId", clusterId)
                .add("proxyTo", proxyTo)
                .add("externalUrl", externalUrl)
                .add("routingGroup", routingGroup)
                .add("userQueuedCount", userQueuedCount)
                .toString();
    }

    public static Builder builder(String clusterId)
    {
        return new Builder(clusterId);
    }

    public static final class Builder
    {
        private final String clusterId;
        private int runningQueryCount;
        private int queuedQueryCount;
        private int numWorkerNodes;
        private boolean healthy;
        private String proxyTo;
        private String externalUrl;
        private String routingGroup;
        private Map<String, Integer> userQueuedCount;

        public Builder(String clusterId)
        {
            this.clusterId = requireNonNull(clusterId, "clusterId is null");
        }

        public Builder runningQueryCount(int runningQueryCount)
        {
            this.runningQueryCount = runningQueryCount;
            return this;
        }

        public Builder queuedQueryCount(int queuedQueryCount)
        {
            this.queuedQueryCount = queuedQueryCount;
            return this;
        }

        public Builder numWorkerNodes(int numWorkerNodes)
        {
            this.numWorkerNodes = numWorkerNodes;
            return this;
        }

        public Builder healthy(boolean healthy)
        {
            this.healthy = healthy;
            return this;
        }

        public Builder proxyTo(String proxyTo)
        {
            this.proxyTo = proxyTo;
            return this;
        }

        public Builder externalUrl(String externalUrl)
        {
            this.externalUrl = externalUrl;
            return this;
        }

        public Builder routingGroup(String routingGroup)
        {
            this.routingGroup = routingGroup;
            return this;
        }

        public Builder userQueuedCount(Map<String, Integer> userQueuedCount)
        {
            this.userQueuedCount = ImmutableMap.copyOf(userQueuedCount);
            return this;
        }

        public ClusterStats build()
        {
            ClusterStats stats = new ClusterStats();
            stats.clusterId(clusterId);
            stats.runningQueryCount(runningQueryCount);
            stats.queuedQueryCount(queuedQueryCount);
            stats.numWorkerNodes(numWorkerNodes);
            stats.healthy(healthy);
            stats.proxyTo(proxyTo);
            stats.externalUrl(externalUrl);
            stats.routingGroup(routingGroup);
            stats.userQueuedCount(userQueuedCount);
            return stats;
        }
    }

    public synchronized void updateLocalStats(String user)
    {
        // The live stats refresh every few seconds, so we update the stats immediately
        // so that they can be used for next queries to route
        // We assume that if a user has queued queries then newly arriving queries
        // for that user would also be queued
        int count = userQueuedCount == null ? 0 : userQueuedCount.getOrDefault(user, 0);
        if (count > 0) {
            userQueuedCount.put(user, count + 1);
            return;
        }
        // Else the we assume that the query would be running
        // so update the clusterstat with the +1 running queries
        ++runningQueryCount;
    }
}
