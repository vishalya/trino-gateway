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
package io.trino.gateway.ha.module;

import com.google.inject.Provides;
import io.dropwizard.core.setup.Environment;
import io.trino.gateway.baseapp.AppModule;
import io.trino.gateway.ha.config.HaGatewayConfiguration;
import io.trino.gateway.ha.persistence.JdbcConnectionManager;
import io.trino.gateway.ha.router.GatewayBackendManager;
import io.trino.gateway.ha.router.HaGatewayManager;
import io.trino.gateway.ha.router.HaQueryHistoryManager;
import io.trino.gateway.ha.router.HaResourceGroupsManager;
import io.trino.gateway.ha.router.QueryHistoryManager;
import io.trino.gateway.ha.router.ResourceGroupsManager;

public class RouterBaseModule
        extends AppModule<HaGatewayConfiguration, Environment>
{
    final ResourceGroupsManager resourceGroupsManager;
    final GatewayBackendManager gatewayBackendManager;
    final QueryHistoryManager queryHistoryManager;
    final JdbcConnectionManager connectionManager;

    public RouterBaseModule(HaGatewayConfiguration configuration, Environment environment)
    {
        super(configuration, environment);
        connectionManager = new JdbcConnectionManager(configuration.getDataStore());
        resourceGroupsManager = new HaResourceGroupsManager(connectionManager);
        gatewayBackendManager = new HaGatewayManager(connectionManager);
        queryHistoryManager = new HaQueryHistoryManager(connectionManager);
    }

    @Provides
    public JdbcConnectionManager getConnectionManager()
    {
        return this.connectionManager;
    }

    @Provides
    public ResourceGroupsManager getResourceGroupsManager()
    {
        return this.resourceGroupsManager;
    }

    @Provides
    public GatewayBackendManager getGatewayBackendManager()
    {
        return this.gatewayBackendManager;
    }

    @Provides
    public QueryHistoryManager getQueryHistoryManager()
    {
        return this.queryHistoryManager;
    }
}
