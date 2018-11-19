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
package com.facebook.presto.hive.metastore.thrift;

import javax.inject.Inject;

import java.net.URI;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class StaticHiveCluster
        implements HiveCluster
{
    private final HiveMetastoreClientFactory clientFactory;
    private final String metastoreUsername;

    @Inject
    public StaticHiveCluster(StaticMetastoreConfig config, HiveMetastoreClientFactory clientFactory)
    {
        this(config.getMetastoreUris(), config.getMetastoreUsername(), clientFactory);
    }

    public StaticHiveCluster(List<URI> metastoreUris, String metastoreUsername, HiveMetastoreClientFactory clientFactory)
    {
        requireNonNull(metastoreUris, "metastoreUris is null");
        checkArgument(!metastoreUris.isEmpty(), "metastoreUris must specify at least one URI");
        this.metastoreUsername = metastoreUsername;
        this.clientFactory = requireNonNull(clientFactory, "clientFactory is null");
    }

    /**
     * Create a metastore client connected to the Hive metastore.
     * <p>
     * As per Hive HA metastore behavior, return the first metastore in the list
     * list of available metastores (i.e. the default metastore) if a connection
     * can be made, else try another of the metastores at random, until either a
     * connection succeeds or there are no more fallback metastores.
     */
    @Override
    public HiveMetastoreClient createMetastoreClient()
    {
        return null;
    }
}
