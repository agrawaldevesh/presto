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
package com.facebook.presto.pinot;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import com.linkedin.pinot.common.data.Schema;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static com.google.common.cache.CacheLoader.asyncReloading;
import static java.util.Objects.requireNonNull;

public class PinotConnection
{
    private static final Object ALL_TABLES_CACHE_KEY = new Object();

    private final LoadingCache<String, List<PinotColumn>> pinotTableColumnCache;
    private final LoadingCache<String, PinotTable> pinotTableCache;
    private final LoadingCache<Object, List<String>> allTablesCache;

    private final PinotClusterInfoFetcher pinotClusterInfoFetcher;

    @Inject
    public PinotConnection(PinotClusterInfoFetcher pinotClusterInfoFetcher, PinotConfig pinotConfig, @ForPinot Executor executor)
    {
        final long cacheExpiryMs = pinotConfig.getMetadataCacheExpiry().roundTo(TimeUnit.MILLISECONDS);
        this.pinotClusterInfoFetcher = requireNonNull(pinotClusterInfoFetcher, "cluster info fetcher is null");
        this.allTablesCache = CacheBuilder.newBuilder()
                .refreshAfterWrite(cacheExpiryMs, TimeUnit.MILLISECONDS)
                .build(asyncReloading(CacheLoader.from(pinotClusterInfoFetcher::getAllTables), executor));

        this.pinotTableCache =
                CacheBuilder.newBuilder()
                        .refreshAfterWrite(cacheExpiryMs, TimeUnit.MILLISECONDS)
                        .build(asyncReloading(new CacheLoader<String, PinotTable>()
                        {
                            @Override
                            public PinotTable load(String tableName)
                                    throws Exception
                            {
                                List<PinotColumn> columns = getPinotColumnsForTable(tableName);
                                return new PinotTable(tableName, columns);
                            }
                        }, executor));

        this.pinotTableColumnCache =
                CacheBuilder.newBuilder()
                        .refreshAfterWrite(cacheExpiryMs, TimeUnit.MILLISECONDS)
                        .build(asyncReloading(new CacheLoader<String, List<PinotColumn>>()
                        {
                            @Override
                            public List<PinotColumn> load(String tableName)
                                    throws Exception
                            {
                                Schema tablePinotSchema = pinotClusterInfoFetcher.getTableSchema(tableName);
                                return PinotColumnUtils.getPinotColumnsForPinotSchema(tablePinotSchema);
                            }
                        }, executor));

        executor.execute(() -> {
            this.allTablesCache.refresh(ALL_TABLES_CACHE_KEY);
        });
    }

    private static <K, V> V getFromCache(LoadingCache<K, V> cache, K key)
            throws Exception
    {
        V v = cache.getIfPresent(key);
        if (v != null) {
            return v;
        }
        return cache.get(key);
    }

    public List<String> getTableNames()
    {
        try {
            return getFromCache(allTablesCache, ALL_TABLES_CACHE_KEY);
        }
        catch (Exception e) {
            throw new PinotException(PinotErrorCode.PINOT_UNCLASSIFIED_ERROR, Optional.empty(), "Cannot fetch tables", e);
        }
    }

    public PinotTable getTable(String tableName)
    {
        try {
            return getFromCache(pinotTableCache, tableName);
        }
        catch (Exception e) {
            throw new PinotException(PinotErrorCode.PINOT_UNCLASSIFIED_ERROR, Optional.empty(), "Error when getting table " + tableName, e);
        }
    }

    private List<PinotColumn> getPinotColumnsForTable(String tableName)
            throws Exception
    {
        return getFromCache(pinotTableColumnCache, tableName);
    }

    public Map<String, Map<String, List<String>>> getRoutingTable(String tableName)
            throws Exception
    {
        // This is not cached because we want to read the new segments as soon as they are available
        return pinotClusterInfoFetcher.getRoutingTableForTable(tableName);
    }

    public Map<String, String> getTimeBoundary(String tableName)
    {
        return pinotClusterInfoFetcher.getTimeBoundaryForTable(tableName);
    }
}
