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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.linkedin.pinot.common.metrics.BrokerMetrics;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.InstanceRequest;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.core.common.datatable.DataTableFactory;
import com.linkedin.pinot.pql.parsers.Pql2CompilationException;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;
import com.linkedin.pinot.serde.SerDe;
import com.linkedin.pinot.transport.common.CompositeFuture;
import com.linkedin.pinot.transport.metrics.NettyClientMetrics;
import com.linkedin.pinot.transport.netty.PooledNettyClientResourceManager;
import com.linkedin.pinot.transport.pool.KeyedPool;
import com.linkedin.pinot.transport.pool.KeyedPoolImpl;
import com.linkedin.pinot.transport.scattergather.ScatterGather;
import com.linkedin.pinot.transport.scattergather.ScatterGatherImpl;
import com.linkedin.pinot.transport.scattergather.ScatterGatherRequest;
import com.linkedin.pinot.transport.scattergather.ScatterGatherStats;
import com.yammer.metrics.core.MetricsRegistry;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.HashedWheelTimer;
import org.apache.thrift.protocol.TCompactProtocol;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static com.facebook.presto.pinot.PinotErrorCode.PINOT_INSUFFICIENT_SERVER_RESPONSE;
import static com.facebook.presto.pinot.PinotErrorCode.PINOT_INVALID_PQL_GENERATED;
import static com.facebook.presto.pinot.PinotErrorCode.PINOT_UNCLASSIFIED_ERROR;
import static com.facebook.presto.pinot.PinotUtils.doWithRetries;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;

public class PinotScatterGatherQueryClient
{
    private static final Logger log = Logger.get(PinotScatterGatherQueryClient.class);
    private static final Pql2Compiler REQUEST_COMPILER = new Pql2Compiler();
    private static final String PRESTO_HOST_PREFIX = "presto-pinot-master";
    private static final boolean DEFAULT_EMIT_TABLE_LEVEL_METRICS = true;

    private final String prestoHostId;
    private final MetricsRegistry registry;
    private final BrokerMetrics brokerMetrics;
    private final ScatterGather scatterGatherer;
    // Netty Specific
    private EventLoopGroup eventLoopGroup;
    private PooledNettyClientResourceManager resourceManager;
    // Connection Pool Related
    private KeyedPool<PooledNettyClientResourceManager.PooledClientConnection> connPool;
    private ScheduledThreadPoolExecutor poolTimeoutExecutor;
    private ExecutorService requestSenderPool;

    @Inject
    public PinotScatterGatherQueryClient(PinotConfig pinotConfig)
    {
        prestoHostId = getDefaultPrestoId();

        registry = new MetricsRegistry();
        brokerMetrics = new BrokerMetrics(registry, DEFAULT_EMIT_TABLE_LEVEL_METRICS);
        brokerMetrics.initializeGlobalMeters();

        eventLoopGroup = new NioEventLoopGroup();
        /**
         * Some of the client metrics uses histogram which is doing synchronous operation.
         * These are fixed overhead per request/response.
         * TODO: Measure the overhead of this.
         */
        final NettyClientMetrics clientMetrics = new NettyClientMetrics(registry, "presto_pinot_client_");

        // Setup Netty Connection Pool
        resourceManager = new PooledNettyClientResourceManager(eventLoopGroup, new HashedWheelTimer(), clientMetrics);

        requestSenderPool = Executors.newFixedThreadPool(pinotConfig.getThreadPoolSize());
        poolTimeoutExecutor = new ScheduledThreadPoolExecutor(50);
        connPool = new KeyedPoolImpl<>(pinotConfig.getMinConnectionsPerServer(), pinotConfig.getMaxConnectionsPerServer(), pinotConfig.getIdleTimeout().toMillis(),
                pinotConfig.getMaxBacklogPerServer(), resourceManager, poolTimeoutExecutor, requestSenderPool, registry);
        resourceManager.setPool(connPool);

        // Setup ScatterGather
        scatterGatherer = new ScatterGatherImpl(connPool, requestSenderPool);
    }

    private String getDefaultPrestoId()
    {
        String defaultBrokerId;
        try {
            defaultBrokerId = PRESTO_HOST_PREFIX + InetAddress.getLocalHost().getHostName();
        }
        catch (UnknownHostException e) {
            log.error("Caught exception while getting default broker id", e);
            defaultBrokerId = PRESTO_HOST_PREFIX;
        }
        return defaultBrokerId;
    }

    public Map<ServerInstance, DataTable> queryPinotServerForDataTable(String pql, String serverHost, List<String> segments, Duration connectionTimeout, boolean ignoreEmptyResponses, int pinotRetryCount)
    {
        BrokerRequest brokerRequest;
        try {
            brokerRequest = REQUEST_COMPILER.compileToBrokerRequest(pql);
        }
        catch (Pql2CompilationException e) {
            throw new PinotException(PINOT_INVALID_PQL_GENERATED, Optional.of(pql),
                    format("Parsing error on %s, Error = %s", serverHost, e.getMessage()), e);
        }

        Map<String, List<String>> routingTable = ImmutableMap.of(serverHost, ImmutableList.copyOf(segments));

        // Unfortunately the retries will all hit the same server because the routing decision has already been made by the pinot broker
        Map<ServerInstance, byte[]> serverResponseMap = doWithRetries(pinotRetryCount, (requestId) -> {
            ScatterGatherRequestImpl scatterRequest = new ScatterGatherRequestImpl(brokerRequest, routingTable, requestId, connectionTimeout.toMillis(), prestoHostId);

            ScatterGatherStats scatterGatherStats = new ScatterGatherStats();
            CompositeFuture<byte[]> compositeFuture = routeScatterGather(scatterRequest, scatterGatherStats);

            if (compositeFuture == null) {
                // No server found in either OFFLINE or REALTIME table.
                throw new PinotException(PINOT_UNCLASSIFIED_ERROR, Optional.of(pql), format("Failed to send read request to %s", serverHost));
            }

            return gatherServerResponses(pql, ignoreEmptyResponses, routingTable, compositeFuture, scatterGatherStats, true, brokerRequest.getQuerySource().getTableName());
        });
        return deserializeServerResponses(pql, serverResponseMap, true, brokerRequest.getQuerySource().getTableName());
    }

    @Nullable
    private Map<ServerInstance, byte[]> gatherServerResponses(
            String pql,
            boolean ignoreEmptyResponses,
            Map<String, List<String>> routingTable,
            @Nonnull CompositeFuture<byte[]> compositeFuture,
            @Nonnull ScatterGatherStats scatterGatherStats, boolean isOfflineTable,
            @Nonnull String tableNameWithType)
    {
        try {
            Map<ServerInstance, byte[]> serverResponseMap = compositeFuture.get();
            if (!ignoreEmptyResponses) {
                if (serverResponseMap.size() != routingTable.size()) {
                    Map<String, Serializable> routingTableForLogging = routingTable.entrySet().stream().collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().size() > 10 ? format("%d segments", entry.getValue().size()) : ImmutableList.copyOf(entry.getValue())));
                    throw new PinotException(PINOT_INSUFFICIENT_SERVER_RESPONSE, Optional.of(pql), String.format("%d of %d servers responded, routing table servers: %s, error: %s", serverResponseMap.size(), routingTable.size(), routingTableForLogging, ImmutableMap.copyOf(compositeFuture.getError())));
                }
                Iterator<Map.Entry<ServerInstance, byte[]>> iterator = serverResponseMap.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<ServerInstance, byte[]> entry = iterator.next();
                    if (entry.getValue().length == 0) {
                        throw new PinotException(PINOT_INSUFFICIENT_SERVER_RESPONSE, Optional.of(pql), String.format("Got empty response from server: %s", entry.getKey().getShortHostName()));
                    }
                }
            }
            Map<ServerInstance, Long> responseTimes = compositeFuture.getResponseTimes();
            scatterGatherStats.setResponseTimeMillis(responseTimes, isOfflineTable);
            return serverResponseMap;
        }
        catch (ExecutionException | InterruptedException e) {
            Throwable err = e instanceof ExecutionException ? ((ExecutionException) e).getCause() : e;
            throw new PinotException(PINOT_UNCLASSIFIED_ERROR, Optional.of(pql), String.format("Caught exception while fetching responses for table: %s", tableNameWithType), err);
        }
    }

    /**
     * Deserialize the server responses, put the de-serialized data table into the data table map passed in, append
     * processing exceptions to the processing exception list passed in.
     * <p>For hybrid use case, multiple responses might be from the same instance. Use response sequence to distinguish
     * them.
     *
     * @param responseMap map from server to response.
     * @param isOfflineTable whether the responses are from an OFFLINE table.
     * @param tableNameWithType table name with type suffix.
     * @return dataTableMap map from server to data table.
     */
    private Map<ServerInstance, DataTable> deserializeServerResponses(
            String pql,
            @Nonnull Map<ServerInstance, byte[]> responseMap, boolean isOfflineTable,
            @Nonnull String tableNameWithType)
    {
        Map<ServerInstance, DataTable> dataTableMap = new HashMap<>();
        for (Map.Entry<ServerInstance, byte[]> entry : responseMap.entrySet()) {
            ServerInstance serverInstance = entry.getKey();
            byte[] value = entry.getValue();
            if (value == null || value.length == 0) {
                continue;
            }
            if (!isOfflineTable) {
                serverInstance = serverInstance.withSeq(1);
            }
            try {
                dataTableMap.put(serverInstance, DataTableFactory.getDataTable(value));
            }
            catch (IOException e) {
                throw new PinotException(PINOT_UNCLASSIFIED_ERROR, Optional.of(pql), String.format("Caught exceptions while deserializing response for table: %s from server: %s", tableNameWithType, serverInstance), e);
            }
        }
        return dataTableMap;
    }

    private CompositeFuture<byte[]> routeScatterGather(ScatterGatherRequestImpl scatterRequest, ScatterGatherStats scatterGatherStats)
    {
        try {
            return this.scatterGatherer.scatterGather(scatterRequest, scatterGatherStats, true, brokerMetrics);
        }
        catch (InterruptedException e) {
            throw new PinotException(PINOT_UNCLASSIFIED_ERROR, Optional.empty(), format("Interrupted while sending request: %s", scatterRequest), e);
        }
    }

    private static class ScatterGatherRequestImpl
            implements ScatterGatherRequest
    {
        private final BrokerRequest brokerRequest;
        private final Map<String, List<String>> routingTable;
        private final long requestId;
        private final long requestTimeoutMs;
        private final String brokerId;

        public ScatterGatherRequestImpl(BrokerRequest request, Map<String, List<String>> routingTable, long requestId, long requestTimeoutMs, String brokerId)
        {
            brokerRequest = request;
            this.routingTable = routingTable;
            this.requestId = requestId;

            this.requestTimeoutMs = requestTimeoutMs;
            this.brokerId = brokerId;
        }

        @Override
        public Map<String, List<String>> getRoutingTable()
        {
            return routingTable;
        }

        @Override
        public byte[] getRequestForService(List<String> segments)
        {
            InstanceRequest r = new InstanceRequest();
            r.setRequestId(requestId);
            r.setEnableTrace(brokerRequest.isEnableTrace());
            r.setQuery(brokerRequest);
            r.setSearchSegments(segments);
            r.setBrokerId(brokerId);
            return new SerDe(new TCompactProtocol.Factory()).serialize(r);
        }

        @Override
        public long getRequestId()
        {
            return requestId;
        }

        @Override
        public long getRequestTimeoutMs()
        {
            return requestTimeoutMs;
        }

        @Override
        public BrokerRequest getBrokerRequest()
        {
            return brokerRequest;
        }

        @Override
        public String toString()
        {
            if (routingTable == null) {
                return null;
            }

            return Arrays.toString(routingTable.entrySet().toArray());
        }
    }
}
